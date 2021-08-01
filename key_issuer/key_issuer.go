package main

import "math/rand"

type KeyIssuer interface {
	GetKey(logicalKey float64) float64
}

type RandomKeyIssuer struct{}

func NewRandomKeyIssuer() *RandomKeyIssuer {
	return &RandomKeyIssuer{}
}

func (ki *RandomKeyIssuer) GetKey(logicalKey float64) float64 {
	return rand.Float64()
}

type AsIsKeyIssuer struct{}

func NewAsIsKeyIssuer() *AsIsKeyIssuer {
	return &AsIsKeyIssuer{}
}

func (ki *AsIsKeyIssuer) GetKey(logicalKey float64) float64 {
	return logicalKey
}

func NewKeyIssuer(issuerType string) KeyIssuer {
	switch issuerType {
	case "random":
		return NewRandomKeyIssuer()
	case "asis":
		return NewAsIsKeyIssuer()
	default:
		return NewShuffleKeyIssuer()
	}
}
