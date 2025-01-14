# DBReader
Database files reader. Directly read data from database files. No need for SQL server, DLLs, ODBC and other API.

Supported databases:
* Interbase 6, 7 (.GDB, .FDB)
* Firebird 2.1 up to 3.0 (.GDB, .FDB)
* Midas.dll/DataSnap/ClientDataSet (.CDS)
* Paradox 3.0 up to 7.x (.DB)
* dBASE/FoxPRO (.DBF)
* MS SQL Server (.MDF, .BAK)
* Mapsoft data export (.GSR)
* MS Access/MS Jet 4 (.MDB, .ACCDB)

Sample projects:
* Database browser - browse tables and inpect values

Dependencies:
* NovaLib (logger, RFUtils, MemStreams) - https://github.com/serbod/NovaLib
* DelphiZLib 1.2.8 (optional)