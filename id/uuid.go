package id

import "os/exec"

/**
 *@Author tudou
 *@Date 2020/12/30
 **/

func LinuxUuidGen()[]byte{
	uuid,err:=exec.Command("uuidgen").Output()
	if err!=nil{
		panic(err)
	}
	return uuid
}