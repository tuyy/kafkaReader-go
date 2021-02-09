package description

import (
	"encoding/base64"
	"fmt"
	"github.com/tuyy/kafkaReader-go/pkg/args"
	"strings"
	"testing"
)

func TestDecryptPayload(t *testing.T) {
	input := "dx84B3tY+W5XCeKVgd3w4Fg/DyfJ5zjCDdFsWVm1rgYaTvszMe+4awEa2Rqt+O6Rf2NmsgYQ4JdrC2pM4y+PbydgoMYiVcsA8R/hWmCumkATLlL56ckweW8z5z7L8xNU7tWVyvSlkvxaVZDAZgJrwXpFwmVW1LGsd2LONcKO8wt/DshZMAKy64GMUWmD2xuBwp8wfIG0Oi/AFOrXlBE8EiecVMaSjc+ai3CqfXGy2ebOcor8MSC1Yb1U7FreOUQLt3m53uyijgM79M7vX4NroHLZT7tc2B39gOJ91gXXyCb7n22PyeE+zbNKAHfLFXzZTbC8ev8w7HmYXJxzJS0QlZCXM6+mfjosUihymsQ/gYQ="
	want := "[2021-01-29 00:33:35.135] CALLER=\"MW\" CALLER_IP=\"10.116.234.203\" USER_IP=\"117.111.1.114\" USER_ID=\"wish125\" ACTION=\"options.Add\" COMMAND=\"ADD\" CLASS=\"SearchHistory\" NAME=\"RecentList\" VALUESN=\"613\" PREVIOUS_VALUE=\"\" CURRENT_VALUE=\"{\"all\":\"대상\"}\" host=cix03-2.nm"

	b, err := base64.StdEncoding.DecodeString(input)
	if err != nil {
		t.Fatal(err)
	}

	key := makeMd5Key("nvmail")

	result, err := decryptAes128Ecb(key, b)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(string(unpad([]byte(result))))

	rz := strings.TrimSpace(result)
	if rz != want {
		t.Fatalf("invalid result. rz:%v want:%v", rz, want)
	}
}

func TestDecryptPayload2(t *testing.T) {
	args.Args.DecryptKey = "nvmail"

	input := "dx84B3tY+W5XCeKVgd3w4Fg/DyfJ5zjCDdFsWVm1rgYaTvszMe+4awEa2Rqt+O6Rf2NmsgYQ4JdrC2pM4y+PbydgoMYiVcsA8R/hWmCumkATLlL56ckweW8z5z7L8xNU7tWVyvSlkvxaVZDAZgJrwXpFwmVW1LGsd2LONcKO8wt/DshZMAKy64GMUWmD2xuBwp8wfIG0Oi/AFOrXlBE8EiecVMaSjc+ai3CqfXGy2ebOcor8MSC1Yb1U7FreOUQLt3m53uyijgM79M7vX4NroHLZT7tc2B39gOJ91gXXyCb7n22PyeE+zbNKAHfLFXzZTbC8ev8w7HmYXJxzJS0QlZCXM6+mfjosUihymsQ/gYQ="
	want := "[2021-01-29 00:33:35.135] CALLER=\"MW\" CALLER_IP=\"10.116.234.203\" USER_IP=\"117.111.1.114\" USER_ID=\"wish125\" ACTION=\"options.Add\" COMMAND=\"ADD\" CLASS=\"SearchHistory\" NAME=\"RecentList\" VALUESN=\"613\" PREVIOUS_VALUE=\"\" CURRENT_VALUE=\"{\"all\":\"대상\"}\" host=cix03-2.nm\n"

	rz, err := DecryptPayload(input)
	if err != nil {
		t.Fatal(err)
	}
	if rz != want {
	    t.Fatalf("invalid result. rz:%v want:%v", rz, want)
	}
}
