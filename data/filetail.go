package data

import (
	"fmt"
	"os"
	"time"
)

func TrimFileTail(path string, maxBytes int) (int, error) {
	if maxBytes <= 0 {
		return 0, nil
	}
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if len(raw) <= maxBytes {
		return 0, nil
	}
	starts := []int{0}
	for i, b := range raw {
		if b == '\n' && i+1 < len(raw) {
			starts = append(starts, i+1)
		}
	}
	idx := len(starts) - 1
	for i := len(starts) - 1; i >= 0; i-- {
		if len(raw)-starts[i] <= maxBytes {
			idx = i
		} else {
			break
		}
	}
	tail := raw[starts[idx]:]
	header := fmt.Sprintf("<!-- 记忆已裁剪 %s：只保留最近 %d 字节（原 %d 字节） -->\n",
		time.Now().Format("2006-01-02"), len(tail), len(raw))
	if err := writeAtomic(path, append([]byte(header), tail...)); err != nil {
		return 0, err
	}
	return len(raw) - len(tail), nil
}

func FileSize(path string) (int, error) {
	st, err := os.Stat(path)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return int(st.Size()), nil
}
