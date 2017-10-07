package libkademlia

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"
	mathrand "math/rand"
	"time"
	"sss"
	// "fmt"
)

type VanashingDataObject struct {
	AccessKey  int64
	Ciphertext []byte
	NumberKeys byte
	Threshold  byte
}

func GenerateRandomCryptoKey() (ret []byte) {
	for i := 0; i < 32; i++ {
		ret = append(ret, uint8(mathrand.Intn(256)))
	}
	return
}

func GenerateRandomAccessKey() (accessKey int64) {
	r := mathrand.New(mathrand.NewSource(time.Now().UnixNano()))
	accessKey = r.Int63()
	return
}

func CalculateSharedKeyLocations(accessKey int64, count int64) (ids []ID) {
	r := mathrand.New(mathrand.NewSource(accessKey))
	ids = make([]ID, count)
	for i := int64(0); i < count; i++ {
		for j := 0; j < IDBytes; j++ {
			ids[i][j] = uint8(r.Intn(256))
		}
	}
	return
}

func encrypt(key []byte, text []byte) (ciphertext []byte) {
	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err)
	}
	ciphertext = make([]byte, aes.BlockSize+len(text))
	iv := ciphertext[:aes.BlockSize]
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		panic(err)
	}
	stream := cipher.NewCFBEncrypter(block, iv)
	stream.XORKeyStream(ciphertext[aes.BlockSize:], text)
	return
}

func decrypt(key []byte, ciphertext []byte) (text []byte) {
	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err)
	}
	if len(ciphertext) < aes.BlockSize {
		panic("ciphertext is not long enough")
	}
	iv := ciphertext[:aes.BlockSize]
	ciphertext = ciphertext[aes.BlockSize:]

	stream := cipher.NewCFBDecrypter(block, iv)
	stream.XORKeyStream(ciphertext, ciphertext)
	return ciphertext
}

func (k *Kademlia) VanishData(data []byte, numberKeys byte,
	threshold byte, timeoutSeconds int) (vdo VanashingDataObject) {
	Key := GenerateRandomCryptoKey()
	m_keys,err := sss.Split(numberKeys, threshold, Key)
	if err != nil {
		return
	}
	var keys [][]byte
	for k,v := range m_keys {
		item := append([]byte{k},v...)
		keys = append(keys,item)
	}

	L := GenerateRandomAccessKey()
	locs := CalculateSharedKeyLocations(L, int64(numberKeys))
	for i:=0;i<len(locs);i++ {
		// fmt.Println("i, locs, keys:",i, locs[i],keys[i])
		_, err := k.DoIterativeStore(locs[i],keys[i])
		if err != nil {
			return
		}
	}

    vdo.AccessKey = L
	vdo.Ciphertext = encrypt(Key,data)
	vdo.NumberKeys = numberKeys
	vdo.Threshold = threshold
	return
}

func (k *Kademlia) UnvanishData(vdo VanashingDataObject) (data []byte) {
	locs := CalculateSharedKeyLocations(vdo.AccessKey, int64(vdo.NumberKeys))
	m_keys := make(map[byte][]byte)
	
	for _,loc := range locs {
		// fmt.Println("<><><><><>",loc)
		value, err := k.DoIterativeFindValue(loc)
		// fmt.Println("<><><><><><><>", value)
		if err == nil {
			m_keys[value[0]] = value[1:]
		}
	}
	if len(m_keys) >= int(vdo.Threshold) {
		Key := sss.Combine(m_keys)
		data = decrypt(Key, vdo.Ciphertext)
		return
	}
	// data = decrypt(Key, vdo.Ciphertext)
	return nil
}
