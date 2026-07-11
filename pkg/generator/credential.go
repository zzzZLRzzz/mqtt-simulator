package generator

import (
	"fmt"

	"conn-conductor/pkg/config"
)

type CredentialGenerator interface {
	GenerateClientID(index int) string
	GenerateUsername(index int) string
	GeneratePassword(index int) string
}

type DefaultCredentialGenerator struct {
	ClientIDPrefix string
	UsernamePrefix string
	PasswordPrefix string
	StaticClientID string
	StaticUsername string
	StaticPassword string
}

func NewDefaultCredentialGenerator(cfg config.Creds) *DefaultCredentialGenerator {
	return &DefaultCredentialGenerator{
		ClientIDPrefix: cfg.ClientIDPrefix,
		UsernamePrefix: cfg.UsernamePrefix,
		PasswordPrefix: cfg.PasswordPrefix,
		StaticClientID: cfg.ClientID,
		StaticUsername: cfg.Username,
		StaticPassword: cfg.Password,
	}
}

func (g *DefaultCredentialGenerator) GenerateClientID(index int) string {
	if g.StaticClientID != "" {
		return g.StaticClientID
	}
	return fmt.Sprintf("%s%d", g.ClientIDPrefix, index)
}

func (g *DefaultCredentialGenerator) GenerateUsername(index int) string {
	if g.StaticUsername != "" {
		return g.StaticUsername
	}
	return fmt.Sprintf("%s%d", g.UsernamePrefix, index)
}

func (g *DefaultCredentialGenerator) GeneratePassword(index int) string {
	if g.StaticPassword != "" {
		return g.StaticPassword
	}
	return fmt.Sprintf("%s%d", g.PasswordPrefix, index)
}
