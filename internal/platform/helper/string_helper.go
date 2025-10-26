package helper

func TrimZeroBytes(data []byte) []byte {
	n := 0
	for {
		if n >= len(data) {
			break
		}
		if data[n] == 0 {
			break
		}
		n++
	}
	res := make([]byte, n)
	copy(res, data[:n])
	return res
}

func ToString(data []byte) string {
	trimmed := TrimZeroBytes(data)
	str := ""
	for _, v := range trimmed {
		str += string(v)
	}
	return str
}
