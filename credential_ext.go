package rocketmq

import (
	"time"
	"fmt"
	"math/rand"
	"reflect"
	"crypto/sha1"
)

type CredentialExt struct {
	accessKey   string
	securityKey string
}

const accessSource string = "MqCloud"

func NewCredentialExt(accessKey string, securityKey string) (RPCHook) {

	return &CredentialExt{
		accessKey:   accessKey,
		securityKey: securityKey,
	}
}

func (c *CredentialExt) DoBeforeRequest(remoteAddr string, request *RemotingCommand) {

	if (request.ExtFields == nil) {
		request.ExtFields = c.extendFields()
	} else {
		switch request.ExtFields.(type) {
		case *SendMessageRequestHeader:

			var header = (reflect.ValueOf(request.ExtFields)).Elem()
			var imap = c.extendFields()
			header.FieldByName("AccessSource").SetString(imap["accessSource"].(string));
			header.FieldByName("AccessKey").SetString(imap["accessKey"].(string));
			header.FieldByName("Timestamp").SetString(imap["timestamp"].(string));
			header.FieldByName("RandomNum").SetString(imap["randomNum"].(string));
			header.FieldByName("Signature").SetString(imap["signature"].(string));

			break
		case *PullMessageRequestHeader:

			var header = (reflect.ValueOf(request.ExtFields)).Elem()
			var imap = c.extendFields()
			header.FieldByName("AccessSource").SetString(imap["accessSource"].(string));
			header.FieldByName("AccessKey").SetString(imap["accessKey"].(string));
			header.FieldByName("Timestamp").SetString(imap["timestamp"].(string));
			header.FieldByName("RandomNum").SetString(imap["randomNum"].(string));
			header.FieldByName("Signature").SetString(imap["signature"].(string));

			break
		case *GetConsumerListByGroupRequestHeader:

			var header = (reflect.ValueOf(request.ExtFields)).Elem()
			var imap = c.extendFields()
			header.FieldByName("AccessSource").SetString(imap["accessSource"].(string));
			header.FieldByName("AccessKey").SetString(imap["accessKey"].(string));
			header.FieldByName("Timestamp").SetString(imap["timestamp"].(string));
			header.FieldByName("RandomNum").SetString(imap["randomNum"].(string));
			header.FieldByName("Signature").SetString(imap["signature"].(string));

			break
		case *UpdateConsumerOffsetRequestHeader:

			var header = (reflect.ValueOf(request.ExtFields)).Elem()
			var imap = c.extendFields()
			header.FieldByName("AccessSource").SetString(imap["accessSource"].(string));
			header.FieldByName("AccessKey").SetString(imap["accessKey"].(string));
			header.FieldByName("Timestamp").SetString(imap["timestamp"].(string));
			header.FieldByName("RandomNum").SetString(imap["randomNum"].(string));
			header.FieldByName("Signature").SetString(imap["signature"].(string));

			break
		case *QueryConsumerOffsetRequestHeader:

			var header = (reflect.ValueOf(request.ExtFields)).Elem()
			var imap = c.extendFields()
			header.FieldByName("AccessSource").SetString(imap["accessSource"].(string));
			header.FieldByName("AccessKey").SetString(imap["accessKey"].(string));
			header.FieldByName("Timestamp").SetString(imap["timestamp"].(string));
			header.FieldByName("RandomNum").SetString(imap["randomNum"].(string));
			header.FieldByName("Signature").SetString(imap["signature"].(string));

			break
		case *GetRouteInfoRequestHeader:

			var header = (reflect.ValueOf(request.ExtFields)).Elem()
			var imap = c.extendFields()
			header.FieldByName("AccessSource").SetString(imap["accessSource"].(string));
			header.FieldByName("AccessKey").SetString(imap["accessKey"].(string));
			header.FieldByName("Timestamp").SetString(imap["timestamp"].(string));
			header.FieldByName("RandomNum").SetString(imap["randomNum"].(string));
			header.FieldByName("Signature").SetString(imap["signature"].(string));

			break
		case *map[string]interface{}:

			var header = request.ExtFields.(map[string]interface{})
			var imap = c.extendFields()
			header["accessSource"] = imap["accessSource"]
			header["accessKey"] = imap["accessKey"]
			header["timestamp"] = imap["timestamp"]
			header["randomNum"] = imap["randomNum"]
			header["signature"] = imap["signature"]
			request.ExtFields = header
			break
		default:
			request.ExtFields = c.extendFields()
			break
		}
	}

}

func (c *CredentialExt) DoBeforeResponse(remoteAddr string, request *RemotingCommand, response *RemotingCommand) {
	if NoPermission == response.Code {
		fmt.Println(response.ExtFields.(map[string]interface{})["message"])
	}

}

func signature(str string) string {
	h := sha1.New()
	h.Write([]byte(str))
	bs := h.Sum(nil)
	return fmt.Sprintf("%x", bs)
}


func (c *CredentialExt) extendFields() (map[string]interface{}) {
	result := make(map[string]interface{})
	result["accessSource"] = accessSource
	result["accessKey"] = c.accessKey
	now := time.Now()
	result["timestamp"] = fmt.Sprintf("%d", (now.UnixNano() / 1e6))
	result["randomNum"] = fmt.Sprintf("%d",  rand.Int())
	result["signature"] = signature(c.accessKey + ";" + result["randomNum"].(string) + ";" + c.securityKey + ";" + result["timestamp"].(string))

	//result.put("signature",SignatureUtil.encrypt(result.get("accessKey") + ";" + result.get("randomNum") + ";" + securityKey + ";" + result.get("timestamp")));

	return result;
}
