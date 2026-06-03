k6 run `
>>   -o experimental-prometheus-rw `
>>   -e API_URL="http://192.168.1.101:30080" `
>>   load-generation-test.js

         /\      Grafana   /‾‾/
    /\  /  \     |\  __   /  /
   /  \/    \    | |/ /  /   ‾‾\
  /          \   |   (  |  (‾)  |
 / __________ \  |_|\_\  \_____/


     execution: local
        script: load-generation-test.js
        output: Prometheus remote write (http://192.168.1.101:30090/api/v1/write)

     scenarios: (100.00%) 1 scenario, 200 max VUs, 3m30s max duration (incl. graceful stop):
              * default: Up to 200 looping VUs for 3m0s over 3 stages (gracefulRampDown: 30s, gracefulStop: 30s)



  █ TOTAL RESULTS

    checks_total.......: 4243    23.305814/s
    checks_succeeded...: 0.00%   0 out of 4243
    checks_failed......: 100.00% 4243 out of 4243

    ✗ score update status is 200
      ↳  0% — ✓ 0 / ✗ 3432
    ✗ leaderboard fetch status is 200
      ↳  0% — ✓ 0 / ✗ 811

    HTTP
    http_req_duration....: avg=4.6s min=871.5µs  med=2.89s max=15.38s p(90)=12.11s p(95)=13.49s
    http_req_failed......: 100.00% 4243 out of 4243
    http_reqs............: 4243    23.305814/s

    EXECUTION
    iteration_duration...: avg=4.7s min=101.31ms med=2.99s max=15.48s p(90)=12.21s p(95)=13.59s
    iterations...........: 4243    23.305814/s
    vus..................: 1       min=1            max=200
    vus_max..............: 200     min=200          max=200

    NETWORK
    data_received........: 1.2 MB  6.8 kB/s
    data_sent............: 681 kB  3.7 kB/s


