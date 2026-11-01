import amqp from "amqplib";
import dotenv from "dotenv";
import { Resend } from 'resend';
dotenv.config();

const resend = new Resend(process.env.RESEND_API_KEY);

export const startSendOtpConsumer = async () => {
  try {
    const connection = await amqp.connect({
      protocol: "amqp",
      hostname: process.env.Rabbitmq_Host,
      port: 5672,
      username: process.env.Rabbitmq_Username,
      password: process.env.Rabbitmq_Password,
    });

    const channel = await connection.createChannel();
    const queueName = "send-otp";

    await channel.assertQueue(queueName, { durable: true });

    // Process one message at a time to prevent rate limits
    channel.prefetch(1);

    console.log("✅ Mail Service consumer started, listening for otp emails");

    channel.consume(queueName, async (msg) => {
      if (msg) {
        try {
          const { to, subject, body } = JSON.parse(msg.content.toString());

          const { data, error } = await resend.emails.send({
            from: 'ChatBox <patelgood3@gmail.com>',
            to: [to],
            subject: subject,
            text: body,
          });

          if (error) {
            console.error('Failed to send otp via Resend:', error);
            // ACK the message so RabbitMQ removes it instead of looping infinitely
            channel.ack(msg);
            return;
          }

          console.log(`OTP mail sent to ${to}`, data);
          channel.ack(msg);
        } catch (error) {
          console.log("Failed to process message", error);
          channel.ack(msg); // ACK on failure to prevent crash/redelivery loops
        }
      }
    });

  } catch (error) {
    console.error("RabbitMQ connection error:", error);
  }
};