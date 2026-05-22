package com.leetjourney.alert_service.service;

import com.leetjourney.alert_service.entity.Alert;
import com.leetjourney.alert_service.repository.AlertRepository;
import jakarta.mail.internet.MimeMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.mail.MailException;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class EmailService {

    private final JavaMailSender mailSender;
    private final AlertRepository alertRepository;

    public EmailService(JavaMailSender mailSender,
                        AlertRepository alertRepository) {
        this.mailSender = mailSender;
        this.alertRepository = alertRepository;
    }

    public void sendEmail(String to,
                          String subject,
                          String body,
                          Long userId) {

        log.info("Sending professional HTML email to: {}", to);

        try {
            MimeMessage mimeMessage = mailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(mimeMessage, true);

            helper.setTo(to);
            helper.setFrom("invydexter@gmail.com");
            helper.setSubject(subject);

            String htmlBody = """
<!DOCTYPE html>
<html>
<body style="margin:0;padding:0;background:#eef2f7;font-family:'Segoe UI',Arial,sans-serif;">

<div style="max-width:700px;margin:40px auto;background:#ffffff;border-radius:16px;overflow:hidden;box-shadow:0 10px 30px rgba(0,0,0,0.12);">

    <div style="background:linear-gradient(135deg,#0f172a,#2563eb);padding:35px;text-align:center;">
        <h1 style="margin:0;color:white;font-size:30px;font-weight:700;">
            Home Energy Tracker
        </h1>
        <p style="margin-top:10px;color:#dbeafe;font-size:15px;">
            Intelligent Energy Monitoring & Alert Notification System
        </p>
    </div>

    <div style="padding:35px;">

        <div style="background:#fff5f5;border:1px solid #fecaca;border-left:6px solid #dc2626;padding:22px;border-radius:12px;">
            <h2 style="margin:0;color:#b91c1c;font-size:24px;">
                ⚠ Energy Usage Billing Alert
            </h2>
            <p style="margin-top:12px;color:#7f1d1d;font-size:15px;line-height:1.6;">
                High energy consumption detected. Review your usage to avoid higher electricity bills.
            </p>
        </div>

        <h3 style="margin-top:35px;color:#1e293b;font-size:20px;">
            Customer Account Details
        </h3>

        <table style="width:100%;border-collapse:collapse;margin-top:15px;">
            <tr style="background:#f8fafc;">
                <td style="padding:16px;border:1px solid #e2e8f0;"><b>User ID</b></td>
                <td style="padding:16px;border:1px solid #e2e8f0;">{USER_ID}</td>
            </tr>
            <tr>
                <td style="padding:16px;border:1px solid #e2e8f0;"><b>Registered Email</b></td>
                <td style="padding:16px;border:1px solid #e2e8f0;">{EMAIL}</td>
            </tr>
        </table>

        <h3 style="margin-top:35px;color:#1e293b;font-size:20px;">
            Energy Usage & Billing Summary
        </h3>

        <table style="width:100%;border-collapse:collapse;margin-top:15px;font-size:15px;color:#334155;">
            <tr style="background:#f8fafc;">
                <td style="padding:14px;border:1px solid #e2e8f0;"><b>Alert Details</b></td>
                <td style="padding:14px;border:1px solid #e2e8f0;">{BODY}</td>
            </tr>
        </table>

        <div style="margin-top:30px;background:#ecfeff;border-radius:12px;padding:20px;border:1px solid #bae6fd;">
            <h4 style="margin:0;color:#0369a1;">Smart Recommendation</h4>
            <p style="margin-top:10px;color:#0f172a;font-size:14px;line-height:1.6;">
                Reduce runtime of high-power appliances like ACs, heaters, and refrigerators.
                Monitoring your projected monthly bill can help control expenses.
            </p>
        </div>

        <div style="text-align:center;margin-top:35px;">
            <a href="http://localhost:3000"
               style="background:linear-gradient(135deg,#2563eb,#1d4ed8);
               color:white;
               text-decoration:none;
               padding:15px 30px;
               border-radius:10px;
               font-weight:600;
               font-size:15px;">
               View Monitoring Dashboard
            </a>
        </div>

    </div>

    <div style="background:#0f172a;padding:22px;text-align:center;">
        <p style="margin:0;color:#cbd5e1;font-size:13px;">
            © 2026 Home Energy Tracker • Smart Monitoring Platform
        </p>
    </div>

</div>

</body>
</html>
""";

            htmlBody = htmlBody
                    .replace("{USER_ID}", String.valueOf(userId))
                    .replace("{EMAIL}", to)
                    .replace("{BODY}", body.replace("\n", "<br>"));

            helper.setText(htmlBody, true);

            mailSender.send(mimeMessage);

            Alert alertSent = Alert.builder()
                    .sent(true)
                    .createdAt(java.time.LocalDateTime.now())
                    .userId(userId)
                    .build();

            alertRepository.saveAndFlush(alertSent);

            log.info("Professional billing email sent successfully.");

        } catch (MailException e) {
            log.error("Failed to send email", e);

            Alert alertSent = Alert.builder()
                    .sent(false)
                    .createdAt(java.time.LocalDateTime.now())
                    .userId(userId)
                    .build();

            alertRepository.saveAndFlush(alertSent);

        } catch (Exception e) {
            log.error("Unexpected error", e);
        }
    }
}