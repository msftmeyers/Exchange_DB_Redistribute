<#
.SYNOPSIS
    Script to empty a too large or overloaded single or a list of multiple Exchange Mailbox Databases.

.DESCRIPTION
    This script creates Exchange Migration batches and move requests for all mailboxes in a named SourceDB or in SourceDBs
    found in a SourceDB CSV file. It will use destination databases in a stagingDB CSV file as targets for all move requests
    evenly. The script makes sure to distribute all mailboxes to be moved evenly and coordinated into staging databases.
    After the script you simply start and complete all migration batches and MoveRequests.
    Afterwards you can simply re-create the overloaded and now empty source DB(s) files and Log stream with a new and default one.
    You will get empty space in both, DB and Volume, immediately. For this follow-up task, use the script exchange_DBrecreate.ps1
    which you can also find at https://github.com/msftmeyers.
    
    Default values:
    BADITEMLIMIT = 0
    MAILBOXBATCHBLOCKSIZE / Mailbox MoveRequests per MIGBATCH = 250
    ARCHIVEBATCHBLOCKSIZE / Archive MoveRequests per MIGBATCH = 20    
    
.PARAMETER SingleDB
    <required> The DBName, from where all mailboxes should be moved (Default Parameter Set: SingleDB)

.PARAMETER SourceDBsCSV
    <required> The CSVFile, where all source DBNames are listed (Parameter Set: SourceDBsCSV)

.PARAMETER StagingDBsCSV
    <required> The CSVFile, where all staging/empty DBNames are listed

.PARAMETER ForceCreate
    <optional> Only with Parameter ForceCreate, Batches and MoveRequests will be created

.PARAMETER BadItemLimit
    <optional> BadItemLimit value for MigrationBatches and MoveRequests (default = 0)

.PARAMETER EmailAddress
    <optional> EmailAdress for MigrationBatch(es) Notification emails

.PARAMETER MailboxBatchBlockSize
    <optional> Maximum number of Standard Mailbox MoveRequests per Batch (default = 250)

.PARAMETER ArchiveBatchBlockSize
    <optional> Maximum number of Archive Mailbox MoveRequests per Batch (default = 20)

.PARAMETER NoNewProvisioning
    <optional> Source Database(s) will be excluded from Exchange provisioning of new mailboxes

.EXAMPLE
    .\exchange_DBredistribute.ps1 [-SingleDB <DBName>/-SourceDBsCSV <CSVFileName>] [-StagingDBsCSV <CSVFileName>] [-BadItemLimit <value>] [-EmailAddress <emailaddress>] [-MailboxBatchBlockSize <value>] [-ArchiveBatchBlockSize <value>] [-NoNewProvisioning]
   
.NOTES
    Steffen Meyer
    Cloud Solution Architect
    Microsoft Deutschland GmbH
    
    V1.0  12.10.2025 - Initial Version
    V1.2  13.10.2025 - Minor Console Output changes & adding parameters
    V1.3  21.10.2025 - Added ForceCreate parameter for creating Batches and MoveRequests
    V1.4  06.11.2025 - Small param comment corrections & changed the way, a database is excluded from MailboxProvisioning
    V1.5  03.12.2025 - Small corrections, description added
    V1.6  17.03.2026 - Fixed special situation, when Mailbox and Archive are part of the same Redistribute action (you cannot create two Move-Requests of the same name)
    V2.0  18.03.2026 - New calculation of targetdatabase and targetarchivedatabase (better distribution), some script consolidation
#>

[CmdletBinding(DefaultParameterSetName="SingleDB")]
Param(
     [Parameter(Mandatory=$true,ParameterSetName="SingleDB",Position=0,HelpMessage='Insert single source Database name')]
     [ValidateNotNullOrEmpty()]
     [String]$SingleDB,
     [Parameter(Mandatory=$true,ParameterSetName="SourceDBsCSV",Position=0,HelpMessage='Insert CSV Import File with Source Database(s)')]
     [ValidateNotNullOrEmpty()]
     [String]$SourceDBsCSV,
     [Parameter(Mandatory=$true,Position=1,HelpMessage='Insert CSV Import File with Destination Database(s)')]
     [ValidateNotNullOrEmpty()]
     [String]$StagingDBsCSV,
     [Parameter(Mandatory=$false,Position=2,HelpMessage='Only with -ForceCreate, Batches and MoveRequests will be created')]
     [Switch]$ForceCreate,
     [Parameter(Mandatory=$false,Position=3,HelpMessage='Insert BadItemLimit value for move requests (e.g. 10, default = 0)')]
     [Int]$BadItemLimit=0,
     [Parameter(Mandatory=$false,Position=4,HelpMessage='Insert MigBatch Notification EmailAddress')]
     [String]$EmailAddress,
     [Parameter(Mandatory=$false,Position=5,HelpMessage='Insert max. number of Standard Mailbox MoveRequests per MigBatch (e.g. 50, default = 250)')]
     [Int]$MailboxBatchBlockSize=250,
     [Parameter(Mandatory=$false,Position=6,HelpMessage='Insert max. number of Archive Mailbox MoveRequests per MigBatch (e.g. 10, default = 20)')]
     [Int]$ArchiveBatchBlockSize=20,
     [Parameter(Mandatory=$false,Position=7,HelpMessage='The Source Database(s) will be excluded from further mailbox provisioning.')]
     [Switch]$NoNewProvisioning
     )

$version = "V2.0_18.03.2026"

$now = Get-Date -Format G

#Function to fetch all mailboxes pointing to a database
Function Get-ExDBStatistics
{
    [CmdletBinding()]
    Param
    (
        [Parameter(Mandatory,ValueFromPipeline)]
        [String]$SourceDB,
        [Parameter()]
        [ValidateSet('','PublicFolder','Arbitration','Auditlog')]
        [String]$MBXType
    )
    Process
    {
        switch ($MBXType)
        {
            ''
            {
                try
                {
                    $MBX = Get-Mailbox -ResultSize unlimited -IgnoreDefaultScope -Database $SourceDB -ErrorAction Stop -WarningAction SilentlyContinue
                }
                catch
                {
                    Write-Host "`nWe couldn't get a complete mailbox list for $($SourceDB)." -ForegroundColor Red
                    Return
                }
            }
            'PublicFolder'
            {
                try
                {
                    $MBX = Get-Mailbox -ResultSize unlimited -IgnoreDefaultScope -Database $SourceDB -PublicFolder -ErrorAction Stop -WarningAction SilentlyContinue
                }
                catch
                {
                    Write-Host "`nWe couldn't get a complete PublicFolder mailbox list for $($SourceDB)." -ForegroundColor Red
                    Return
                }
            }
            'Arbitration'
            {
                try
                {
                    $MBX = Get-Mailbox -ResultSize unlimited -IgnoreDefaultScope -Database $SourceDB -Arbitration -ErrorAction Stop -WarningAction SilentlyContinue
                }
                catch
                {
                    Write-Host "`nWe couldn't get a complete arbitration mailbox list for $($SourceDB)." -ForegroundColor Red
                    Return
                }
            }
            'AuditLog'
            {
                try
                {
                    $MBX = Get-Mailbox -ResultSize unlimited -IgnoreDefaultScope -Database $SourceDB -AuditLog -ErrorAction Stop -WarningAction SilentlyContinue
                }
                catch
                {
                    Write-Host "`nWe couldn't get a complete AuditLog mailbox list for $($SourceDB)." -ForegroundColor Red
                    Return
                }
            }
        }
        Return $MBX
    }
}

#Function to find all archives pointing to a database
Function Get-ExDBArchives
{
    [CmdletBinding()]
    Param
    (
        [Parameter(Mandatory,ValueFromPipeline)]
        [String]$SourceDB
    )
    Process
    {
        $Archives = $null
        Write-Host "NOTICE: Fetching all ARCHIVE mailboxes in Database $($SourceDB), this may take a while..." -ForegroundColor Yellow

        try
        {
            $Archives = Invoke-Expression "Get-Mailbox -Archive -resultsize unlimited -ignoredefaultscope -ErrorAction Stop -WarningAction SilentlyContinue | where-object Archivedatabase -like $SourceDB"
        }
        catch
        {
            Write-Host "`nWe couldn't get a complete Archive mailbox list for $($SourceDB)." -ForegroundColor Red
        }
    Return $Archives
    }
}

#Function to get MailboxStatistics
Function Get-ExMBXStatistics
{
    [CmdletBinding()]
    Param(
        [Object[]]$Mailboxes,
        [Switch]$Archive
        )
    Process
    {
        #Counter for Progressbar
        $MBXCount = ($Mailboxes).count
        $Count = 0

        #Fetch all mailbox stats
        $Result=@()

        foreach ($Mailbox in $Mailboxes)
        {
            #ProgressBar
            $Count++
            $Activity = "Analyzing Mailboxes... [$($Count)/$($MBXCount)]"
            $Message = "Getting Statistics for Mailbox: $($Mailbox)"
            Write-Progress -Status $Message -Activity $Activity -PercentComplete (($Count/$MBXCount)*100)
        
            #Mailbox stats
            $stat = $null
            $totalitemsize = $null
            $totaldeleteditemsize = $null
            $totalsize = $null
            
            #Archive stats
            $archivestat = $null
            $arctotalitemsize = $null
            $arctotaldeleteditemsize = $null
            $totalarchivesize = $null

            if (!($Archive))
            {
                $stat = Get-MailboxStatistics -Identity $Mailbox.exchangeguid.guid -WarningAction SilentlyContinue -ErrorAction SilentlyContinue
            
                #Mailbox Stats
                if ($stat)
                {
                    $totalitemsize = $stat.totalitemsize.value.tokB()
                    $totaldeleteditemsize = $stat.totaldeleteditemsize.value.tokB()
                    $totalsize = $totalitemsize + $totaldeleteditemsize
                }
            }    
            else
            {
                $archivestat = Get-MailboxStatistics -Archive -Identity $Mailbox.exchangeguid.guid -WarningAction SilentlyContinue -ErrorAction SilentlyContinue
                
                if ($archivestat)
                {
                    $arctotalitemsize = $archivestat.TotalItemSize.value.tokB()
                    $arctotaldeleteditemsize = $archivestat.TotalDeletedItemSize.value.tokB()
                    $totalarchivesize = $arctotalitemsize + $arctotaldeleteditemsize
                }
            }
        
            #Fill up a sorted array    
            $data = [ordered] @{
                EmailAddress = $Mailbox.primarysmtpaddress
                Database = $Mailbox.database
                ArchiveDatabase = $Mailbox.archivedatabase
                TotalitemsizeInkB = $totalitemsize
                TotalDeleteditemsizeInkB = $totaldeleteditemsize
                SUM = $totalsize
                Itemcount = $stat.itemcount
                ArchiveTotalitemsizeInkB = $arctotalitemsize
                ArchiveTotalDeleteditemsizeInkB = $arctotaldeleteditemsize
                ArchiveSUM = $totalarchivesize
                ArchiveItemcount = $archivestat.itemcount
                Name = $Mailbox.samaccountname
                ExGuid = $Mailbox.exchangeguid
                }
    
            #Create object    
            $Result += New-Object -TypeName PSObject -Property $data
        }
        Write-Progress -Activity $Activity -Completed
        Return $Result
    }
}

#START SCRIPT
try
{
    $ScriptPath = Split-Path -Parent $MyInvocation.MyCommand.Path -ErrorAction Stop
}
catch
{
    Write-Host "`nDo not forget to save the script!" -ForegroundColor Red
}

Write-Host "`nScript version: $version"
Write-Host   "Script started: $now   "

Write-Host "`n---------------------------------------------------------------------------------------" -Foregroundcolor Green
Write-Host   "Script to empty a large or overloaded Single or a list of several Source DBs (CSV),    " -Foregroundcolor Green
Write-Host   "find destination databases for all mailboxes, archives and special mailboxes in a      " -Foregroundcolor Green
Write-Host   "CSV file and create migration batch(es) and move requests.                             " -Foregroundcolor Green
Write-Host "`nThe script makes sure to distribute all mailboxes to be moved evenly and coordinated   " -ForegroundColor Green
Write-Host   "into staging databases. After the script you simply start and complete all migration   " -ForegroundColor Green
Write-Host   "batches and MoveRequests and you can simply delete the overloaded source databases     " -ForegroundColor Green
Write-Host   "and create an empty one. You will get empty space in both, DB and Volume, immediately. " -ForegroundColor Green
Write-Host   "---------------------------------------------------------------------------------------" -Foregroundcolor Green

If (!($ForceCreate))
{
    Write-Host "`n---------------------------------------------------------------------------------------" -Foregroundcolor Yellow
    Write-Host   "ATTENTION: You started the script without ""-ForceCreate"" parameter. This means, the  " -ForegroundColor Yellow
    Write-Host   "script will NOT create any Batches or MoveRequests. It will create CSV files for       " -ForegroundColor Yellow
    Write-Host   "Batches for review and will tell you the number of Batches and MoveRequests it would   " -ForegroundColor Yellow
    Write-Host   "create. If all is fine, just restart this script with ""-ForceCreate"" parameter.      " -ForegroundColor Yellow
    Write-Host   "---------------------------------------------------------------------------------------" -Foregroundcolor Yellow
}

#Create .\CSV Subfolder
if (!(Test-Path "$($ScriptPath)\CSV"))
{
    $CSVDir = New-Item -Path "$($ScriptPath)\CSV" -ItemType Directory -Force
}

#SourceDBsCSV
if ($PSCmdlet.ParameterSetName -eq "SourceDBsCSV")
{
    if (Test-Path -Path "$ScriptPath\$SourceDBsCSV")
    {
        $SourceDBs = Get-Content -Path "$ScriptPath\$SourceDBsCSV" | Select-Object -Skip 1 | ForEach-Object {($_ -split ';')[0].Trim('"') } | Sort-Object
    }
    else
    {
        Write-Host "`nThe required source databases file $($SourceDBsCSV) file is missing. Add the file with a list of source DB(s) to ensure a working SCRIPT." -ForegroundColor Red
        Return
    }
}

#Single SourceDB
else
{
    $SourceDBs = @($SingleDB)
}

#StagingDBs.csv
if (Test-Path -Path "$ScriptPath\$StagingDBsCSV")
{
    $StagingDBs = Get-Content -Path "$ScriptPath\$StagingDBsCSV" | Select-Object -Skip 1 | ForEach-Object {($_ -split ';')[0].Trim('"') } | Sort-Object
}
else
{
    Write-Host "`nWe couldn't find the required file ""$($scriptpath)\$($StagingDBsCSV)"". Provide the folder ""CSV"" and add the file with a list of staging/destination DB(s) to ensure a working SCRIPT." -ForegroundColor Red
    Return
}

if (($SourceDBs | Where-Object {$StagingDBs -contains $_ }).count -gt 0)
{
    Write-Host "`nATTENTION: We found same DB name(s) in both, Source AND Staging, that doesn't make sense, please verify, correct and restart script!" -ForegroundColor Red
    Return
}

#Count Source- and Staging Databases
if ($SourceDBs -and $StagingDBs)
{
    $SourceDBsCount = ($SourceDBs).count
    $StagingDBsCount = ($StagingDBs).count
    
    Write-Host "`nDistribution of ALL mailboxes of $SourceDBsCount Source Database(s) into $StagingDBsCount Staging/Destination Database(s) evenly (by MailboxCount AND MailboxSizes)." -ForegroundColor Green
}

#If empty CSVs are found, exit the script
else
{
    if (!($SourceDBs))
    {
        Write-Host "`nATTENTION: We found no Source Database(s), please verify content of SourceDBs-CSV and restart script!" -ForegroundColor Red
        Return
    }
    if (!($StagingDBs))
    {
        Write-Host "`nATTENTION: We found no Staging Database(s), please verify content of StagingDBs-CSV and restart script!" -ForegroundColor Red
        Return
    }
}

#Check if Exchange SnapIn is available and load it
if (!(Get-PSSession).ConfigurationName -eq "Microsoft.Exchange")
{
    if ((Get-PSSnapin -Registered).name -contains "Microsoft.Exchange.Management.PowerShell.SnapIn")
    {
        Write-Host "`nLoading the Exchange Powershell SnapIn..." -ForegroundColor Yellow
        Add-PSSnapin Microsoft.Exchange.Management.PowerShell.SnapIn -ErrorAction SilentlyContinue
        . $env:ExchangeInstallPath\bin\RemoteExchange.ps1
        Connect-ExchangeServer -auto -AllowClobber
    }
    else
    {
        Write-Host "`nExchange Management Tools are not installed. Run the script on a different machine." -ForegroundColor Red
        Return
    }
}

#Detect, where the script is executed
if (!(Get-ExchangeServer -Identity $env:COMPUTERNAME -ErrorAction SilentlyContinue))
{
    Write-Host "`nATTENTION: Script is executed on a non-Exchangeserver..." -ForegroundColor Cyan
}

Set-ADServerSettings -ViewEntireForest $true

#Call MailboxList Function for all Mailbox Types in SourceDB(s)
$Mailboxes = @()
$PFMBXs = @()
$ArbitrationMBXs = @()
$AuditlogMBXs = @()
$ArchiveMBXs = @()

foreach ($SourceDB in $SourceDBs)
{
    Write-Host "`nNOTICE: Getting Mailbox numbers for Database $($SourceDB)..." -ForegroundColor Cyan
    
    if ($NoNewProvisioning -and $ForceCreate)
    {
        $IsExcluded = (Get-MailboxDatabase $SourceDB).distinguishedname | Set-ADObject -Replace @{msExchProvisioningFlags=3}
        Write-Host "NOTICE: Source Database $($SourceDB) was excluded from further Mailbox provisioning successfully."
    }
    $Mailboxes += Get-ExDBStatistics -SourceDB $SourceDB -MBXType ""

    $PFMBXs += Get-ExDBStatistics -SourceDB $SourceDB -MBXType "PublicFolder"

    $ArbitrationMBXs += Get-ExDBStatistics -SourceDB $SourceDB -MBXType "Arbitration"

    $AuditLogMBXs += Get-ExDBStatistics -SourceDB $SourceDB -MBXType "Auditlog"

    $ArchiveMBXs += Get-ExDBArchives -SourceDB $SourceDB
}

#MailboxDB AND ArchiveDB of a particular mailbox are in the list of SourceDBs
$MBXsArchives = $Mailboxes | Where-Object { $ArchiveMBXs.ExchangeGuid -contains $_.ExchangeGuid }

#Summarize mailboxcount to be moved
Write-Host "`nIn your source database(s), we found the following numbers:" -ForegroundColor Green
Write-Host   "$($Mailboxes.Count) Standard mailbox(es)"
Write-Host   "$($ArchiveMBXs.Count) Archive mailbox(es)"
Write-Host   "$($PFMBXs.Count) PublicFolder mailbox(es)"
Write-Host   "$($ArbitrationMBXs.Count) Arbitration mailbox(es)"
Write-Host   "$($AuditLogMBXs.Count) Auditlog mailbox(es)"

#Fetch all MailboxStatistics of all Standard mailboxes of SourceDB(s) into an object sorted by ascending totalsize
if ($Mailboxes)
{
    $MailboxesResult = Get-ExMBXStatistics -Mailboxes $Mailboxes
}

#Fetch all MailboxStatistics of all PublicFolder mailboxes of SourceDB(s) into an object
if ($PFMBXs)
{
    $PFMBXsResult = Get-ExMBXStatistics -Mailboxes $PFMBXs
}

#Fetch all MailboxStatistics of all Arbitration mailboxes of SourceDB(s) into an object
if ($ArbitrationMBXs)
{
    $ArbitrationMBXsResult = Get-ExMBXStatistics -Mailboxes $ArbitrationMBXs
}

#Fetch all MailboxStatistics of all Auditlog mailboxes of SourceDB(s) into an object
if ($AuditLogMBXs)
{
   $AuditLogMBXsResult = Get-ExMBXStatistics -Mailboxes $AuditLogMBXs
}

#Fetch all MailboxStatistics of all Archive mailboxes of SourceDB(s) into an object sorted by ascending totalarchivesize
if ($ArchiveMBXs)
{
    $ArchiveMBXsResult = Get-ExMBXStatistics -Mailboxes $ArchiveMBXs -Archive
}

#Summarize Stats
$AllMBXsResult = @()

#Standard Mailboxes
if ($Mailboxes)
{
    $AllMBXsResult += $MailboxesResult | ForEach-Object {
        [PSCustomObject]@{
            Emailaddress             = $_.EmailAddress
            Database                 = $_.Database
            Totalitemsize            = $_.TotalitemsizeInkB
            TotalDeleteditemsizeInkB = $_.TotalDeleteditemsizeInkB
            SUM                      = $_.SUM
            Itemcount                = $_.Itemcount
            Name                     = $_.Name
            ExGuid                   = $_.ExGuid
            MailboxType              = "User"
            }
        }
}

#Archive Mailboxes
if ($ArchiveMBXs)
{
    $AllMBXsResult += $ArchiveMBXsResult | ForEach-Object {
        [PSCustomObject]@{
            Emailaddress             = $_.EmailAddress
            Database                 = $_.ArchiveDatabase
            Totalitemsize            = $_.ArchiveTotalitemsizeInkB
            TotalDeleteditemsizeInkB = $_.ArchiveTotalDeleteditemsizeInkB
            SUM                      = $_.ArchiveSUM
            Itemcount                = $_.ArchiveItemcount
            Name                     = $_.Name
            ExGuid                   = $_.ExGuid
            MailboxType              = "Archive"
            }
        }
}

#PublicFolder Mailboxes
if ($PFMBXs)
{
    $AllMBXsResult += $PFMBXsResult | ForEach-Object {
        [PSCustomObject]@{
            Emailaddress             = $_.EmailAddress
            Database                 = $_.Database
            Totalitemsize            = $_.TotalitemsizeInkB
            TotalDeleteditemsizeInkB = $_.TotalDeleteditemsizeInkB
            SUM                      = $_.SUM
            Itemcount                = $_.Itemcount
            Name                     = $_.Name
            ExGuid                   = $_.ExGuid
            MailboxType              = "PublicFolder"
            }
        }
}

#Arbitration Mailboxes
if ($ArbitrationMBXs)
{
    $AllMBXsResult += $ArbitrationMBXsResult | ForEach-Object {
        [PSCustomObject]@{
            Emailaddress             = $_.EmailAddress
            Database                 = $_.Database
            Totalitemsize            = $_.TotalitemsizeInkB
            TotalDeleteditemsizeInkB = $_.TotalDeleteditemsizeInkB
            SUM                      = $_.SUM
            Itemcount                = $_.Itemcount
            Name                     = $_.Name
            ExGuid                   = $_.ExGuid
            MailboxType              = "Arbitration"
            }
        }
}

#Auditlog Mailboxes
if ($AuditlogMBXs)
{
    $AllMBXsResult += $AuditLogMBXsResult | ForEach-Object {
        [PSCustomObject]@{
            Emailaddress             = $_.EmailAddress
            Database                 = $_.Database
            Totalitemsize            = $_.TotalitemsizeInkB
            TotalDeleteditemsizeInkB = $_.TotalDeleteditemsizeInkB
            SUM                      = $_.SUM
            Itemcount                = $_.Itemcount
            Name                     = $_.Name
            ExGuid                   = $_.ExGuid
            MailboxType              = "Auditlog"
            }
        }
}

#Sort and assign TargetDatabase value from stagingDBs (simple algorithm to forward and backward fill all mailboxes from an ascending order of SUM of totalmailboxsize and totaldeletedmailboxsize
if ($AllMBXsResult)
{
    $AllMBXsResult = $AllMBXsResult | Sort-Object SUM

    $Index = 0
    $Direction = 1
    $MaxIndex = $StagingDBsCount - 1

    $MailboxesObject = foreach ($AllMBXResult in $AllMBXsResult)
    {
        $TargetDatabase = $StagingDBs[$Index]

        [PSCustomObject]@{
        EmailAddress          = $AllMBXResult.EmailAddress
        TargetDatabase        = $(if ($AllMBXResult.MailboxType -ne "Archive") { $TargetDatabase } else { $null })
        TargetArchiveDatabase = $(if ($AllMBXResult.MailboxType -eq "Archive") { $TargetDatabase } else { $null })
        BadItemLimit          = $BadItemLimit
        MailboxType           = $AllMBXResult.MailboxType
        Name                  = $AllMBXResult.Name
        }

        $Next = $Index + $Direction

        if ($Next -gt $MaxIndex -or $Next -lt 0)
        {
            $Direction = -$Direction
        }
        else
        {
            $Index = $Next
        }
    }

    #Create three different types of CSVs and migration batches (Primary only, Archive only and Primary+Archive)

    #Filter out all Mailboxes, only PrimaryMailbox needs to be moved here
    $MailboxesBatchObject = $MailboxesObject | Where-Object { $_.EmailAddress -notin $MBXsArchives.PrimarySmtpAddress -and $_.MailboxType -eq "User"}
    
    $MailboxBatchCounter = 0

    if ($MailboxesBatchObject)
    {
        #Export to CSV file, every CSV will not consist of more than $MailboxBatchBlocksize mailboxes
        $MailboxesBatchObject | ForEach-Object -Begin {
            $MailboxBatchCounter++
            $Index = 0
            $Batch = @()
        } -Process {
        $Batch += $_ | Select-Object EmailAddress,TargetDatabase,BadItemLimit
        $Index++

        #always $MailboxBatchBlockSize mailboxes per batch
        if ($Index -eq $MailboxBatchBlocksize)
        {
            $Batch | Export-Csv -Path "$Scriptpath\CSV\redist_mailboxes_$($SourceDBs[0])_$($StagingDBsCSV)_$($MailboxBatchCounter).csv" -NoTypeInformation -Encoding UTF8
        
            if ($ForceCreate)
            {
                Write-Host "`nNOTICE: Creating Standard Mailboxes MigrationBatch $MailboxBatchCounter..." -ForegroundColor Cyan        
                
                try
                {
                    if ($EmailAddress)
                    {
                        $MailboxMigrationBatch = New-MigrationBatch -Name "Redist_Mailboxes_$($SourceDBs[0])_$($StagingDBsCSV)_$($MailboxBatchCounter)" -Local -CSVData ([System.IO.File]::ReadAllBytes("$Scriptpath\CSV\redist_mailboxes_$($SourceDBs[0])_$($StagingDBsCSV)_$($MailboxBatchCounter).csv")) -PrimaryOnly -NotificationEmails $EmailAddress -ErrorAction Stop
                        Write-Host "MigrationBatch ""$($MailboxMigrationBatch.Identity)"" with $Index Standard mailbox(es) created successfully, Batch notifications will be sent to ""$EmailAddress""." -ForegroundColor Green
                    }
                    else
                    {
                        $MailboxMigrationBatch = New-MigrationBatch -Name "Redist_Mailboxes_$($SourceDBs[0])_$($StagingDBsCSV)_$($MailboxBatchCounter)" -Local -CSVData ([System.IO.File]::ReadAllBytes("$Scriptpath\CSV\redist_mailboxes_$($SourceDBs[0])_$($StagingDBsCSV)_$($MailboxBatchCounter).csv")) -PrimaryOnly -ErrorAction Stop
                        Write-Host "MigrationBatch ""$($MailboxMigrationBatch.Identity)"" with $Index Standard mailbox(es) created successfully." -ForegroundColor Green
                    }
                }
                catch
                {
                    Write-Host "Couldn't create MigrationBatch $MailboxBatchCounter for Standard mailboxes." -ForegroundColor Red
                }
            }
            $MailboxBatchCounter++
            $Batch = @()
            $Index = 0
        }
    
        #put remaining mailboxes into final batch
        } -End {
            if ($Batch.Count -gt 0)
            {
                $Batch | Export-Csv -Path "$Scriptpath\CSV\redist_mailboxes_$($SourceDBs[0])_$($StagingDBsCSV)_$($MailboxBatchCounter).csv" -NoTypeInformation -Encoding UTF8
            
                if ($ForceCreate)
                {
                    Write-Host "`nNOTICE: Creating Standard Mailboxes MigrationBatch $MailboxBatchCounter..." -ForegroundColor Cyan

                    try
                    {
                        if ($EmailAddress)
                        {
                            $MailboxMigrationBatch = New-MigrationBatch -Name "Redist_Mailboxes_$($SourceDBs[0])_$($StagingDBsCSV)_$($MailboxBatchCounter)" -Local -CSVData ([System.IO.File]::ReadAllBytes("$Scriptpath\CSV\redist_mailboxes_$($SourceDBs[0])_$($StagingDBsCSV)_$($MailboxBatchCounter).csv")) -PrimaryOnly -NotificationEmails $EmailAddress -ErrorAction Stop
                            Write-Host "MigrationBatch ""$($MailboxMigrationBatch.Identity)"" with $Index Standard mailbox(es) created successfully, Batch notifications will be sent to ""$EmailAddress""." -ForegroundColor Green
                        }
                        else
                        {
                            $MailboxMigrationBatch = New-MigrationBatch -Name "Redist_Mailboxes_$($SourceDBs[0])_$($StagingDBsCSV)_$($MailboxBatchCounter)" -Local -CSVData ([System.IO.File]::ReadAllBytes("$Scriptpath\CSV\redist_mailboxes_$($SourceDBs[0])_$($StagingDBsCSV)_$($MailboxBatchCounter).csv")) -PrimaryOnly -ErrorAction Stop
                            Write-Host "MigrationBatch ""$($MailboxMigrationBatch.Identity)"" with $Index Standard mailbox(es) created successfully." -ForegroundColor Green
                        }
                    }
                    catch
                    {
                        Write-Host "Couldn't create MigrationBatch $MailboxBatchCounter for Standard mailboxes." -ForegroundColor Red
                    }
                }
            }
        }
    }

    #Filter out all Archives, only ArchiveMailbox needs to be moved here
    $ArchiveMBXsBatchObject = $MailboxesObject | Where-Object { $_.EmailAddress -notin $MBXsArchives.PrimarySmtpAddress -and $_.MailboxType -eq "Archive"}
    
    $ArchiveBatchCounter = 0

    if ($ArchiveMBXsBatchObject)
    {
        #Export to CSV file, every CSV will not consist of more than $ArchiveBatchBlocksize mailboxes
        
        $ArchiveMBXsBatchObject | ForEach-Object -Begin {
            $ArchiveBatchCounter++
            $Index = 0
            $Batch = @()
        } -Process {

        $Batch += $_ | Select-Object EmailAddress,TargetArchiveDatabase,BadItemLimit
        $Index++

        #always $ArchiveBatchBlockSize Archive mailboxes per batch
        if ($Index -eq $ArchiveBatchBlocksize)
        {
            $Batch | Export-Csv -Path "$Scriptpath\CSV\redist_archives_$($SourceDBs[0])_$($StagingDBsCSV)_$($ArchiveBatchCounter).csv" -NoTypeInformation -Encoding UTF8
        
            if ($ForceCreate)
            {
                Write-Host "`nNOTICE: Creating Archive Mailboxes MigrationBatch $ArchiveBatchCounter..." -ForegroundColor Cyan

                try
                {
                    if ($EmailAddress)
                    {
                        $ArchiveMigrationBatch = New-MigrationBatch -Name "Redist_Archives_$($SourceDBs[0])_$($StagingDBsCSV)_$($ArchiveBatchCounter)" -Local -CSVData ([System.IO.File]::ReadAllBytes("$Scriptpath\CSV\redist_archives_$($SourceDBs[0])_$($StagingDBsCSV)_$($ArchiveBatchCounter).csv")) -ArchiveOnly -NotificationEmails $EmailAddress -ErrorAction Stop
                        Write-Host "MigrationBatch ""$($ArchiveMigrationBatch.Identity)"" with $Index Archive mailbox(es) created successfully, Batch notifications will be sent to ""$EmailAddress""." -ForegroundColor Green
                    }
                    else
                    {
                        $ArchiveMigrationBatch = New-MigrationBatch -Name "Redist_Archives_$($SourceDBs[0])_$($StagingDBsCSV)_$($ArchiveBatchCounter)" -Local -CSVData ([System.IO.File]::ReadAllBytes("$Scriptpath\CSV\redist_archives_$($SourceDBs[0])_$($StagingDBsCSV)_$($ArchiveBatchCounter).csv")) -ArchiveOnly -ErrorAction Stop
                        Write-Host "MigrationBatch ""$($ArchiveMigrationBatch.Identity)"" with $Index Archive mailbox(es) created successfully" -ForegroundColor Green
                    }
                }
                catch
                {
                    Write-Host "Couldn't create MigrationBatch $ArchiveBatchCounter for Archive mailboxes." -ForegroundColor Red
                }
            }
            $ArchiveBatchCounter++
            $Batch = @()
            $Index = 0
        }
        #put remaining Archive mailboxes into final batch
        } -End {
            if ($Batch.Count -gt 0) {
            
                $Batch | Export-Csv -Path "$Scriptpath\CSV\redist_archives_$($SourceDBs[0])_$($StagingDBsCSV)_$($ArchiveBatchCounter).csv" -NoTypeInformation -Encoding UTF8
        
                if ($ForceCreate)
                {
                    Write-Host "`nNOTICE: Creating Archive Mailboxes MigrationBatch $ArchiveBatchCounter..." -ForegroundColor Cyan

                    try
                    {
                        if ($EmailAddress)
                        {
                            $ArchiveMigrationBatch = New-MigrationBatch -Name "Redist_Archives_$($SourceDBs[0])_$($StagingDBsCSV)_$($ArchiveBatchCounter)" -Local -CSVData ([System.IO.File]::ReadAllBytes("$Scriptpath\CSV\redist_archives_$($SourceDBs[0])_$($StagingDBsCSV)_$($ArchiveBatchCounter).csv")) -ArchiveOnly -NotificationEmails $EmailAddress -ErrorAction Stop
                            Write-Host "MigrationBatch ""$($ArchiveMigrationBatch.Identity)"" with $Index Archive mailbox(es) created successfully, Batch notifications will be sent to ""$EmailAddress""." -ForegroundColor Green
                        }
                        else
                        {
                            $ArchiveMigrationBatch = New-MigrationBatch -Name "Redist_Archives_$($SourceDBs[0])_$($StagingDBsCSV)_$($ArchiveBatchCounter)" -Local -CSVData ([System.IO.File]::ReadAllBytes("$Scriptpath\CSV\redist_archives_$($SourceDBs[0])_$($StagingDBsCSV)_$($ArchiveBatchCounter).csv")) -ArchiveOnly -ErrorAction Stop
                            Write-Host "MigrationBatch ""$($ArchiveMigrationBatch.Identity)"" with $Index Archive mailbox(es) created successfully." -ForegroundColor Green
                        }
                    }
                    catch
                    {
                        Write-Host "Couldn't create MigrationBatch $ArchiveBatchCounter for Archive mailboxes." -ForegroundColor Red
                    }
                }
            }
        }
    }

    #Filter out all Mailboxes, where Primary and ArchiveMailbox are listed in SourceDB(s), both, Primary and Archive needs to be part of the same MoveRequest
    $MailboxesArchivesObject = $MailboxesObject | Where-Object { $_.EmailAddress -in $MBXsArchives.PrimarySmtpAddress} | Group-Object EmailAddress |
        ForEach-Object {
            $group = $_.Group

            [PSCustomObject]@{
                EmailAddress          = $_.Name
                TargetDatabase        = $group.TargetDatabase | select -First 1
                TargetArchiveDatabase = $group.TargetArchiveDatabase | select -First 1
                BadItemLimit          = $BadItemLimit
                }
            }
        
    #Create a CSV import file for MigrationBatch(es) for particular Mailboxes where Database and TargetDatabase are in the source DB(s) (MigrationBatch/MoveRequest with both, Primary + Archive)
    $MailboxArchiveBatchCounter = 0
    
    if ($MailboxesArchivesObject)
    {
        #Export to CSV file, every CSV will not consist of more than $ArchiveBatchBlocksize mailboxes
        $MailboxesArchivesObject | ForEach-Object -Begin {
            $MailboxArchiveBatchCounter++
            $Index = 0
            $Batch = @()
        } -Process {
        
        $Batch += $_ | Select-Object EmailAddress,TargetDatabase,TargetArchiveDatabase,BadItemLimit
        $Index++

        #always $ArchiveBatchBlockSize mailboxes per batch
        if ($Index -eq $ArchiveBatchBlockSize)
        {
            $Batch | Export-Csv -Path "$Scriptpath\CSV\redist_mailboxes_archives_$($SourceDBs[0])_$($StagingDBsCSV)_$($MailboxArchiveBatchCounter).csv" -NoTypeInformation -Encoding UTF8
        
            if ($ForceCreate)
            {
                Write-Host "`nNOTICE: Creating Standard Mailboxes AND Archives MigrationBatch $MailboxArchiveBatchCounter..." -ForegroundColor Cyan        
                
                try
                {
                    if ($EmailAddress)
                    {
                        $MailboxArchiveMigrationBatch = New-MigrationBatch -Name "Redist_Mailboxes_Archives_$($SourceDBs[0])_$($StagingDBsCSV)_$($MailboxArchiveBatchCounter)" -Local -CSVData ([System.IO.File]::ReadAllBytes("$Scriptpath\CSV\redist_mailboxes_archives_$($SourceDBs[0])_$($StagingDBsCSV)_$($MailboxArchiveBatchCounter).csv")) -NotificationEmails $EmailAddress -ErrorAction Stop
                        Write-Host "MigrationBatch ""$($MailboxArchiveMigrationBatch.Identity)"" with $Index Standard AND Archive mailbox(es) created successfully, Batch notifications will be sent to ""$EmailAddress""." -ForegroundColor Green
                    }
                    else
                    {
                        $MailboxArchiveMigrationBatch = New-MigrationBatch -Name "Redist_Mailboxes_Archives_$($SourceDBs[0])_$($StagingDBsCSV)_$($MailboxArchiveBatchCounter)" -Local -CSVData ([System.IO.File]::ReadAllBytes("$Scriptpath\CSV\redist_mailboxes_archives_$($SourceDBs[0])_$($StagingDBsCSV)_$($MailboxArchiveBatchCounter).csv")) -ErrorAction Stop
                        Write-Host "MigrationBatch ""$($MailboxArchiveMigrationBatch.Identity)"" with $Index Standard AND Archive mailbox(es) created successfully." -ForegroundColor Green
                    }
                }
                catch
                {
                    Write-Host "Couldn't create MigrationBatch $MailboxArchiveBatchCounter for Standard AND Archive mailboxes." -ForegroundColor Red
                }
            }
            $MailboxArchiveBatchCounter++
            $Batch = @()
            $Index = 0
        }
        #put remaining mailboxes into final batch
        } -End {
            if ($Batch.Count -gt 0) {
            
                $Batch | Export-Csv -Path "$Scriptpath\CSV\redist_mailboxes_archives_$($SourceDBs[0])_$($StagingDBsCSV)_$($MailboxArchiveBatchCounter).csv" -NoTypeInformation -Encoding UTF8

                if ($ForceCreate)
                {
                    Write-Host "`nNOTICE: Creating Standard Mailboxes AND Archives MigrationBatch $MailboxArchiveBatchCounter..." -ForegroundColor Cyan

                    try
                    {
                        if ($EmailAddress)
                        {
                            $MailboxArchiveMigrationBatch = New-MigrationBatch -Name "Redist_Mailboxes_Archives_$($SourceDBs[0])_$($StagingDBsCSV)_$($MailboxArchiveBatchCounter)" -Local -CSVData ([System.IO.File]::ReadAllBytes("$Scriptpath\CSV\redist_mailboxes_archives_$($SourceDBs[0])_$($StagingDBsCSV)_$($MailboxArchiveBatchCounter).csv")) -NotificationEmails $EmailAddress -ErrorAction Stop
                            Write-Host "MigrationBatch ""$($MailboxArchiveMigrationBatch.Identity)"" with $Index Standard AND Archive mailbox(es) created successfully, Batch notifications will be sent to ""$EmailAddress""." -ForegroundColor Green
                        }
                        else
                        {
                            $MailboxArchiveMigrationBatch = New-MigrationBatch -Name "Redist_Mailboxes_Archives_$($SourceDBs[0])_$($StagingDBsCSV)_$($MailboxArchiveBatchCounter)" -Local -CSVData ([System.IO.File]::ReadAllBytes("$Scriptpath\CSV\redist_mailboxes_archives_$($SourceDBs[0])_$($StagingDBsCSV)_$($MailboxArchiveBatchCounter).csv")) -ErrorAction Stop
                            Write-Host "MigrationBatch ""$($MailboxArchiveMigrationBatch.Identity)"" with $Index Standard AND Archive mailbox(es) created successfully." -ForegroundColor Green
                        }
                    }
                    catch
                    {
                        Write-Host "Couldn't create MigrationBatch $MailboxArchiveBatchCounter for Standard AND Archive mailboxes." -ForegroundColor Red
                    }
                }
            }
        }
    }
}
else
{
    Write-Host "`nNOTICE: We couldn't find any statistics of any User and/or Archive mailbox in any SourceDB(s)." -ForegroundColor Yellow
}

#Create Move Requests for all PublicFolder Mailboxes into the calculated StagingDatabase
$PFMoveRequestsCount = 0

if ($PFMBXs)
{
    $PFMBXsObject = $MailboxesObject | Where-Object { $_.MailboxType -eq "PublicFolder"}

    $PFMBXsObject | ForEach-Object {
    
        if ($ForceCreate)
        {
            Write-Host "`nNOTICE: Creating MoveRequest for PublicFolder Mailbox ""$($_.EmailAddress)""..." -ForegroundColor Cyan

            $MoveRequest = Get-Mailbox -PublicFolder ([String]$_.EmailAddress) | New-MoveRequest -TargetDatabase $_.TargetDatabase -Suspend:$true -BadItemLimit $BadItemLimit -WarningAction SilentlyContinue
            
            Write-Host "Move Request for PublicFolder mailbox ""$($_.EmailAddress)"" created successfully." -ForegroundColor Green
        }
        $PFMoveRequestsCount++
    }
}

#Create Move Requests for all Arbitration mailboxes into the calculated StagingDatabase
$ArbitrationMoveRequestsCount = 0

if ($ArbitrationMBXs)
{
    $ArbitrationMBXsObject = $MailboxesObject | Where-Object { $_.MailboxType -eq "Arbitration"}

    $ArbitrationMBXsObject | ForEach-Object {
    
        if ($ForceCreate)
        {
            Write-Host "`nNOTICE: Creating MoveRequest for Arbitration Mailbox ""$($_.EmailAddress)""..." -ForegroundColor Cyan

            $MoveRequest = Get-Mailbox -Arbitration ([String]$_.EmailAddress) | New-MoveRequest -TargetDatabase $_.TargetDatabase -Suspend:$true -BadItemLimit $BadItemLimit -WarningAction SilentlyContinue

            Write-Host "Move Request for Arbitration mailbox ""$($_.EmailAddress)"" created successfully." -ForegroundColor Green
        }
        $ArbitrationMoveRequestsCount++
    }
}

#Create Move Requests for all Auditlog mailboxes into the calculated StagingDatabase
$AuditLogMoveRequestsCount = 0

if ($AuditLogMBXs)
{
    $AuditLogMBXsObject = $MailboxesObject | Where-Object { $_.MailboxType -eq "AuditLog"}

    $AuditLogMBXsObject | ForEach-Object {
        
        if ($ForceCreate)
        {
            Write-Host "`nNOTICE: Creating MoveRequest for AuditLog Mailbox ""$($_.EmailAddress)""..." -ForegroundColor Cyan

            $MoveRequest = Get-Mailbox -AuditLog ([String]$_.EmailAddress) | New-MoveRequest -TargetDatabase $_.TargetDatabase -Suspend:$true -BadItemLimit $BadItemLimit -WarningAction SilentlyContinue

            Write-Host "Move Request for Auditlog mailbox ""$($_.EmailAddress)"" created successfully." -ForegroundColor Green
        }
        $AuditLogMoveRequestsCount++
    }
}

#Final statement for manual steps to follow for empty and re-create Source Database(s)
Write-Host "`nPLEASE READ CAREFULLY:" -ForegroundColor Yellow
Write-Host   "------------------------------------------------------------------------------------------------------------------------------"

if ($ForceCreate) {Write-Host "This script created:"} else {Write-Host "This script would create:"}
Write-Host "`n$($MailboxBatchCounter) MigrationBatch(es) for $((@($MailboxesBatchObject)).Count) Standard mailbox(es),                      " -ForegroundColor Cyan
Write-Host   "$($ArchiveBatchCounter) MigrationBatch(es) for $((@($ArchiveMBXsBatchObject)).Count) Archive mailbox(es),                     " -ForegroundColor Cyan
Write-Host   "$($MailboxArchiveBatchCounter) MigrationBatch(es) for $((@($MailboxesArchivesObject)).Count) Standard AND Archive mailbox(es)," -ForegroundColor Cyan
Write-Host   "$($PFMoveRequestsCount) MoveRequest(s) for $((@($PFMBXsObject)).Count) PublicFolder mailbox(es),                              " -ForegroundColor Cyan
Write-Host   "$($ArbitrationMoveRequestsCount) MoveRequest(s) for $((@($ArbitrationMBXsObject)).Count) Arbitration Mailbox(es),             " -ForegroundColor Cyan
Write-Host   "$($AuditLogMoveRequestsCount) MoveRequest(s) for $((@($AuditLogMBXsObject)).Count) Auditlog mailbox(es)                       " -ForegroundColor Cyan
Write-Host "`nof the selected SourceDatabase(s).                                                                                            "

if ($ForceCreate)
{
    Write-Host "`nYou need to start AND complete the batches MANUALLY and you need to resume/start the                  " -ForegroundColor Yellow
    Write-Host   "MoveRequests MANUALLY, (ATTENTION!) MoveRequests will be completed automatically.                     " -ForegroundColor Yellow
    Write-Host "`nAfter it, the selected Source database(s) is/are empty and the EDB files can be safely                "
    Write-Host   "deleted to free up space in the volume and to reduce the size of the database files without           "
    Write-Host   "using legacy offline database defragmention.                                                          "
    Write-Host   "Do not forget to initial re-seed all copies and, if legacy Exchange backup is in place, to take       "
    Write-Host   "a FULL BACKUP after Database re-creation immediately.                                                 "
    Write-Host   "------------------------------------------------------------------------------------------------------"
}
else
{
    Write-Host "`nIf you want to create Batches and MoveRequests, just restart the script with parameter ""-ForceCreate""" -ForegroundColor Yellow
}
#END SCRIPT