REM GDAL Initialization Script
call ..\release-1911-x64-gdal-3-0-2-mapserver-7-4-2\SDKShell.bat setenv

REM Write Grids to DSS
..\python-embed-amd64\python .\c2dss.py ^
    --outfile C:\data\dss\forecast.dss ^
    --datetime-after 20110101-0000 ^
    --datetime-before 20110103-0000 ^
    --basins ^
        15e50ede-337b-4bbf-a6fa-1be57d1b8715 ^
        b30c6162-3801-4014-b59a-3224c5a0ab10 ^
    --products ^
        e0baa220-1310-445b-816b-6887465cc94b ^
        86526298-78fa-4307-9276-a7c0a0537d15 ^
        57da96dc-fc5e-428c-9318-19f095f461eb ^
        757c809c-dda0-412b-9831-cb9bd0f62d1d ^
        c2f2f0ed-d120-478a-b38f-427e91ab18e2 ^
    --parallel --processes 8

REM Keep Batch File window open until complete
pause