package utils

import (
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"io"
)

var alpha = []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
var alphaLower = []byte("abcdefghijklmnopqrstuvwxyz")
var numer = []byte("0123456789")

func RandomAlphaNum(length int) string {
	return randChar(1, alpha) + randChar(length-1, append(alpha, numer...))
}

func RandomLowerAlphaNum(length int) string {
	return randChar(1, alphaLower) + randChar(length-1, append(alphaLower, numer...))
}

func randChar(length int, chars []byte) string {
	newPword := make([]byte, length)
	randomData := make([]byte, length+(length/4))
	clen := byte(len(chars))
	maxrb := byte(256 - (256 % len(chars)))
	i := 0
	for {
		if _, err := io.ReadFull(rand.Reader, randomData); err != nil {
			panic(err)
		}
		for _, c := range randomData {
			if c >= maxrb {
				continue
			}
			newPword[i] = chars[c%clen]
			i++
			if i == length {
				return string(newPword)
			}
		}
	}
}

// It is noted that masterPassword and binding username are generated deterministically by this function.
// If you want to change the hashing method, check the implications on creating and dropping users.
func GenerateHash(text string, maxLength int) string {
	hash := sha256.Sum256([]byte(text))
	encoded := base64.URLEncoding.EncodeToString(hash[:])
	if len(encoded) > maxLength {
		return encoded[0:maxLength]
	} else {
		return encoded
	}
}

func GetMD5B64(text string, maxLength int) string {
	md5 := md5.Sum([]byte(text))
	encoded := base64.URLEncoding.EncodeToString(md5[:])
	if len(encoded) > maxLength {
		return encoded[0:maxLength]
	} else {
		return encoded
	}
}
