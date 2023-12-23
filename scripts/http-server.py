import os
from http.server import SimpleHTTPRequestHandler, HTTPServer

handler = SimpleHTTPRequestHandler
server_address = ("", 80)
os.chdir(f"{os.getcwd()}/../extension/httpfs/test/dataset")
try:
    with HTTPServer(server_address, handler) as httpd:
        print("HTTP server is running at http://localhost:80/")
        httpd.serve_forever()
except KeyboardInterrupt:
    print("\nServer terminated by user.")
