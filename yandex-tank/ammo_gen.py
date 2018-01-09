#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys

def make_ammo(method, url, headers, case, body):
    """ makes phantom ammo """
    #http request w/o entity body template
    req_template = (
          "%s %s HTTP/1.1\r\n"
          "%s\r\n"
          "\r\n"
    )

    #http request with entity body template
    req_template_w_entity_body = (
          "%s %s HTTP/1.1\r\n"
          "%s\r\n"
          "Content-Length: %d\r\n"
          "\r\n"
          "%s\r\n"
    )

    if not body:
        req = req_template % (method, url, headers)
    else:
        req = req_template_w_entity_body % (method, url, headers, len(body), body)

    #phantom ammo template
    ammo_template = (
        "%d %s\n"
        "%s"
    )

    return ammo_template % (len(req), case, req)

def main():
    headers_GET = "Host: localhost:8080\r\n" + \
            "User-Agent: tank\r\n" + \
            "Accept: */*\r\n" + \
            "Connection: Close"

    f = open("ammo.txt", "w")
    for i in range(1, 10001):
        

	url = "/v0/entity?id=key{}".format(i)
	case = "get_{}".format(i)
        f.write(make_ammo("GET", url, headers_GET, case, ""))
	body = "value{}".format(i)
        case = "put_{}".format(i)
	headers_PUT = "Host: localhost:8080\r\n" + \
	    "Content-Length: {}\r\n".format(len(body)) + \
            "User-Agent: tank\r\n" + \
            "Accept: */*\r\n" + \
            "Connection: Close"
	f.write(make_ammo("PUT", url, headers_PUT, case, body))
	f.write("\r\n")
    f.close();

if __name__ == "__main__":
    main()
