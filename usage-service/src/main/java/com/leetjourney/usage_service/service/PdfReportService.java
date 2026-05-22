package com.leetjourney.usage_service.service;

import com.leetjourney.usage_service.dto.DeviceDto;
import com.leetjourney.usage_service.dto.UsageDto;
import com.lowagie.text.*;
import com.lowagie.text.pdf.PdfWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.time.LocalDate;

@Service
public class PdfReportService {

    private final UsageService usageService;

    @Value("${electricity.rate.per.kwh}")
    private double electricityRate;

    @Value("${monthly.billing.days}")
    private int billingDays;

    public PdfReportService(UsageService usageService) {
        this.usageService = usageService;
    }

    public byte[] generateUsageReport(Long userId, int days) {

        UsageDto usage = usageService.getXDaysUsageForUser(userId, days);

        if (usage.devices() == null || usage.devices().isEmpty()) {
            throw new RuntimeException("No usage data found");
        }


        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            Document document = new Document();
            PdfWriter.getInstance(document, outputStream);

            document.open();

            Font titleFont = new Font(Font.HELVETICA, 20, Font.BOLD);
            Font normalFont = new Font(Font.HELVETICA, 12);

            document.add(new Paragraph("HOME ENERGY TRACKER REPORT", titleFont));
            document.add(new Paragraph(" "));
            document.add(new Paragraph("User ID: " + userId, normalFont));
            document.add(new Paragraph("Generated On: " + LocalDate.now(), normalFont));
            document.add(new Paragraph("Report Duration: " + days + " days", normalFont));
            document.add(new Paragraph(" "));

            double totalEnergy = 0.0;

            for (DeviceDto device : usage.devices()) {
                double deviceCost = (device.energyConsumed() / 1000.0) * electricityRate;

                document.add(new Paragraph(
                        device.name()
                                + " | Usage: "
                                + String.format("%.2f", device.energyConsumed())
                                + " W | Cost: ₹"
                                + String.format("%.2f", deviceCost)
                ));

                totalEnergy += device.energyConsumed();
            }

            double totalKwh = totalEnergy / 1000.0;
            double totalCost = totalKwh * electricityRate;
            double projectedBill = totalCost * billingDays;

            document.add(new Paragraph(" "));
            document.add(new Paragraph("TOTAL SUMMARY", titleFont));
            document.add(new Paragraph(" "));
            document.add(new Paragraph("Total Energy Used: " + String.format("%.2f", totalKwh) + " kWh"));
            document.add(new Paragraph("Estimated Cost: ₹" + String.format("%.2f", totalCost)));
            document.add(new Paragraph("Projected Monthly Bill: ₹" + String.format("%.2f", projectedBill)));

            document.close();

            return outputStream.toByteArray();

        } catch (Exception e) {
            throw new RuntimeException("Failed to generate PDF report", e);
        }
    }
}