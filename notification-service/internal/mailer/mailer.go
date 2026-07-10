package mailer

import (
	"fmt"
	"net/smtp"
)

type Mailer struct {
	addr string
	from string
}

func New(host, port string) *Mailer {
	return &Mailer{
		addr: fmt.Sprintf("%s:%s", host, port),
		from: "notifications@aegis-banking.local",
	}
}

func (m *Mailer) Send(to, subject, body string) error {
	msg := fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s\r\n",
		m.from, to, subject, body)

	if err := smtp.SendMail(m.addr, nil, m.from, []string{to}, []byte(msg)); err != nil {
		return fmt.Errorf("failed to send mail to %s: %w", to, err)
	}
	return nil
}
