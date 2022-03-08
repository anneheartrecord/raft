package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"log"
)

func RSADecrypt(src []byte, prvkey []byte) (res []byte, err error) {
	//解码
	block, _ := pem.Decode(prvkey)
	blockBytes := block.Bytes
	//x509 解码私钥
	privateKey, err := x509.ParsePKCS1PrivateKey(blockBytes)
	if err != nil {
		log.Fatalf("rsa parse private key failed,%v\n", err)
	}
	//还原数据
	res, err = rsa.DecryptPKCS1v15(rand.Reader, privateKey, src)

	if err != nil {
		log.Fatalf("rsa decypt failed,%v\n", err)
	}
	return
}

func RSAEncrypt(src []byte, pubkey []byte) (res []byte, err error) {
	block, _ := pem.Decode(pubkey)
	//解码公钥
	keyInit, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		log.Fatalf("rsa parse public key failed,%v\n", err)
	}
	publicKey := keyInit.(*rsa.PublicKey)
	//加密公钥
	res, err = rsa.EncryptPKCS1v15(rand.Reader, publicKey, src)
	if err != nil {
		log.Fatalf("rsa encrypt failed,%v\n", err)
	}
	return
}
