package sqlengine

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"io"
)

func encryptString(keyStr, plaintext string) (string, error) {
	cipher, err := buildCipher(keyStr)
	if err != nil {
		return "", err
	}
	encrypted, err := makeNonce(cipher.NonceSize())
	if err != nil {
		return "", err
	}

	encrypted = cipher.Seal(encrypted, encrypted[:cipher.NonceSize()], []byte(plaintext), nil)
	return base64.URLEncoding.EncodeToString(encrypted), nil
}

func decryptString(keyStr, ciphertextStr string) (string, error) {
	ciphertext, err := base64.URLEncoding.DecodeString(ciphertextStr)
	if err != nil {
		return "", err
	}

	cipher, err := buildCipher(keyStr)
	if err != nil {
		return "", err
	}

	decrypted, err := cipher.Open(nil, ciphertext[:cipher.NonceSize()], ciphertext[cipher.NonceSize():], nil)
	if err != nil {
		return "", err
	}
	return string(decrypted), nil
}

func buildCipher(keyStr string) (cipher.AEAD, error) {
	key := sha256.Sum256([]byte(keyStr))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func makeNonce(size int) ([]byte, error) {
	nonce := make([]byte, size)
	_, err := io.ReadFull(rand.Reader, nonce)
	return nonce, err
}
