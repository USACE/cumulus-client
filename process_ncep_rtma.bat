REM GDAL Initialization Script
call ..\release-1911-x64-gdal-3-0-2-mapserver-7-4-2\SDKShell.bat setenv

REM Write Grids to DSS
..\python-embed-amd64\python .\c2dss.py ^
    --outfile C:\data\dss\forecast.dss ^
    --datetime-after 20200101-0000 ^
    --datetime-before 20200201-0000 ^
    --basins ^
        15e50ede-337b-4bbf-a6fa-1be57d1b8715 ^
    --products ^
        816abf9e-d9b8-4ba8-9532-78e36409b0b0 ^
    --parallel --processes 8

REM Keep Batch File window open until complete
pause