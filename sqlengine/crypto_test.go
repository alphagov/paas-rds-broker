package sqlengine

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("crypto functions", func() {
	const (
		key  = "some secret key"
		text = "a secret message"
	)

	It("can encrypt and decrypt a value", func() {
		encrypted, err := encryptString(key, text)
		Expect(err).NotTo(HaveOccurred())
		Expect(encrypted).NotTo(Equal(text))

		decrypted, err := decryptString(key, encrypted)
		Expect(err).NotTo(HaveOccurred())
		Expect(decrypted).To(Equal(text))
	})

	It("generates different ciphertext each time", func() {
		// It's important that symmetric encryption uses an IV or nonce so that
		// it's not possible to obtain information about the key by comparing 2
		// or more different ciphertexts.
		encrypted1, err := encryptString(key, text)
		Expect(err).NotTo(HaveOccurred())

		encrypted2, err := encryptString(key, text)
		Expect(err).NotTo(HaveOccurred())

		Expect(encrypted2).NotTo(Equal(encrypted1))
	})

	It("isn't possible to decrypt with the wrong key", func() {
		encrypted, err := encryptString(key, text)
		Expect(err).NotTo(HaveOccurred())

		_, err = decryptString("not the key", encrypted)
		Expect(err).To(MatchError(ContainSubstring("message authentication failed")))
	})
})
