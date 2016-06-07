package utils_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/alphagov/paas-rds-broker/utils"
)

var _ = Describe("RandomAlphaNum", func() {
	It("generates a random alpha numeric with the proper length", func() {
		randomString := RandomAlphaNum(32)
		Expect(len(randomString)).To(Equal(32))
	})
})

var _ = Describe("GetMD5B64", func() {
	It("returns the Base64 encoded MD5 hash of the given string", func() {
		md5b64 := GetMD5B64("ce71b484-d542-40f7-9dd4-5526e38c81ba", 32)
		// Expectation generated with
		// echo -n ce71b484-d542-40f7-9dd4-5526e38c81ba | openssl dgst -md5 -binary | openssl enc -base64
		Expect(md5b64).To(Equal("OzUBBVyWFqGmb7pb54mPVQ=="))
	})

	It("truncates the result when it's longer than the resuested max size", func() {
		md5b64 := GetMD5B64("ce71b484-d542-40f7-9dd4-5526e38c81ba", 16)
		Expect(md5b64).To(Equal("OzUBBVyWFqGmb7pb"))
	})
})
