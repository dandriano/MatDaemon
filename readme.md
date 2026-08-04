# MatDaemon

Parfor processor (dockerized parallel computing toolbox)

## Usage

1. Ensure matlab environment
2. Generate some matlab function, like `Multiply.m`:
```
function result = Multiply(a, b)
    % A simple safe function for testing
    result = a * b;
end
```
3. Upload function to the server

```
curl -s -X POST http://localhost:8080/scripts -F "file=@Multiply.m"

should return:
{"message": "Script uploaded and registered", "fname": "Multiply"}
```
4. Submit task to the daemon

```
curl -s -X POST http://localhost:8080/tasks -H 'Content-Type: application/json' -d '{"fname":"Multiply","params":{"a":2,"b":3},"priority":"NORMAL"}'

should return:
{"task_id": "<GUID>", "status_url": "/tasks/<GUID>"}
```

5. Poll for result
`curl -s http://localhost:8080/tasks/<GUID>`