package cmd

import (
	"crypto/aes"
	"crypto/md5"
	"encoding/base64"
	"errors"
)

func decryptAes128Ecb(key, target []byte) (string, error) {
	cipher, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	if len(target) % aes.BlockSize != 0 {
		return "", errors.New("need a multiple of the block size(16)")
	}

	result := make([]byte, len(target))

	tmp := result
	for len(tmp) > 0 {
		cipher.Decrypt(tmp, target)
		tmp = tmp[aes.BlockSize:]
		target = target[aes.BlockSize:]
	}

	return string(result), nil
}

func makeMd5Key(key string) []byte {
	m := md5.New()
	m.Write([]byte(key))
	return m.Sum(nil)
}

var decryptKey []byte

func DecryptPayload(payload string) (string, error) {
	if len(decryptKey) == 0 {
	    decryptKey = makeMd5Key(Args.DecryptKey)
	}

	b, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return "", err
	}

	decrypted, err := decryptAes128Ecb(decryptKey, b)
	if err != nil {
		return "", err
	}

	return decrypted, nil
}

