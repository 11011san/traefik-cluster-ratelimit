package redis

import (
	"fmt"
	"strings"
)

type Script interface {
	Run(keys []string, args ...interface{}) (interface{}, error)
}

type ScriptImpl struct {
	client    *ClientImpl
	script    string
	scriptSha string
}

func (r *ClientImpl) NewScript(script string) Script {
	rs := &ScriptImpl{
		client:    r,
		script:    script,
		scriptSha: "",
	}

	return rs
}

func (rs *ScriptImpl) Run(keys []string, args ...interface{}) (interface{}, error) {
	conn, err := rs.client.get()
	if err != nil {
		return "", err
	}
	defer rs.client.put(conn)

	argsarray, err := convertToStringArray(args)
	if err != nil {
		return "", err
	}

	params := []string{"EVALSHA", rs.scriptSha}
	params = append(params, fmt.Sprintf("%d", len(keys)))
	params = append(params, keys...)
	params = append(params, argsarray...)

	present := false
	if rs.scriptSha != "" {
		res, err := sendCommand(conn, rs.client.connectionTimeout, params...)
		if err != nil {
			// let's reset the conn
			conn.Close()
			conn = nil
			return "", err
		}
		if res.Success == RESP_SUCCESS_WITH_RESULT {
			return res.Result, nil
		}
		if res.Success == RESP_SUCCESS_WITH_RESULTS {
			return res.Results, nil
		}
		if res.Success == RESP_FAIL || strings.HasPrefix(res.Result.(string), "NOSCRIPT") {
			fmt.Println("not able to run the script:", res.Result)
			return "", fmt.Errorf("not able to run the script: %s", res.Result)
		} else {
			fmt.Println("not able toget script result:", res.Success)
			return "", fmt.Errorf("not able toget script result: %d", res.Success)
		}
	}

	// the script was not present, let's load it and runs it
	if !present {
		res, err := sendCommand(conn, rs.client.connectionTimeout, "SCRIPT", "LOAD", rs.script)
		if err != nil {
			// let's reset the conn
			conn.Close()
			conn = nil
			return "", err
		}
		// sha
		if len(res.Result.(string)) == 40 {
			rs.scriptSha = res.Result.(string)
		} else {
			fmt.Println("not able to load script:", res.Result)
			return "", fmt.Errorf("not able to load script: %s", res.Result)
		}
	}

	params = []string{"EVALSHA", rs.scriptSha}
	params = append(params, fmt.Sprintf("%d", len(keys)))
	params = append(params, keys...)
	params = append(params, argsarray...)

	// run the script
	res, err := sendCommand(conn, rs.client.connectionTimeout, params...)
	if err != nil {
		// let's reset the conn
		conn.Close()
		conn = nil
		return "", err
	}
	if res.Success == RESP_SUCCESS_WITH_RESULT {
		return res.Result, nil
	}
	if res.Success == RESP_SUCCESS_WITH_RESULTS {
		return res.Results, nil
	}
	if res.Success == RESP_FAIL || strings.HasPrefix(res.Result.(string), "NOSCRIPT") {
		fmt.Println("not able to run the script:", res.Result)
		return "", fmt.Errorf("not able to run the script: %s", res.Result)
	} else {
		fmt.Println("not able toget script result:", res.Success)
		return "", fmt.Errorf("not able toget script result: %d", res.Success)
	}
}

func convertToStringArray(args ...interface{}) ([]string, error) {
	// Initialize a slice to hold the string values
	result := make([]string, len(args))

	// Loop through the interface slice and convert each element to a string
	for i, arg := range args {
		switch v := arg.(type) {
		case string:
			result[i] = v
		default:
			// Convert non-string types to string using fmt.Sprintf
			result[i] = fmt.Sprintf("%v", arg)
		}
	}

	return result, nil
}
