<#
.SYNOPSIS
    Script to empty a too large or overloaded single or a list of several Source DBs, find destination databases
    for all mailboxes, archives and pf mailboxes in a CSV file and create a single or multiple migration batch(es)
    with all move requests. The script makes sure to distribute all mailboxes to be moved evenly and coordinated
    into staging databases. After the script you simply start and complete all migration batches and MoveRequests
    and you can simply delete the past overloaded and now empty source DB(s) and create an empty one.
    You will get empty space in both, DB and Volume, immediately.
    
.PARAMETER SingleDB
    <required> The DBName, from where all mailboxes should be moved

.PARAMETER SourceDBsCSV
    <required> The CSVFile, where all source DBNames are listed

.PARAMETER StagingDBsCSV
    <required> The CSVFile, where all staging/empty DBNames are listed

.PARAMETER ForceCreate
    <optional> Only with Parameter ForceCreate, Batches and MoveRequests will be created

.PARAMETER BadItemLimit
    <optional> BadItemLimit value for MigrationBatches and MoveRequests (default = 0)

.PARAMETER EmailAddress
    <optional> EmailAdress for MigrationBatch(es) Notification emails

.PARAMETER MailboxBatchBlockSize
    <optional> Maximum Number of Standard Mailbox MoveRequests per Batch (default = 250)

.PARAMETER ArchiveBatchBlockSize
    <optional> Maximum Number of Archive Mailbox MoveRequests per Batch (default = 20)

.PARAMETER NoNewProvisioning
    <optional> Source Database(s) will be excluded from Exchange provisioning of new mailboxes

.EXAMPLE
    .\exchange_DBredistribute.ps1 [-SingleDB <DBName>/-SourceDBsCSV <CSVFileName>] [-StagingDBsCSV <CSVFileName>] [-BadItemLimit <value>] [-EmailAddress <emailaddress>] [-MailboxBatchBlockSize <value>] [-ArchiveBatchBlockSize <value>] [-NoNewProvisioning]

.VERSIONS
    V1.0 12.10.2025 - Initial Version
    V1.2 13.10.2025 - Minor Console Output changes & adding parameters
    V1.3 21.10.2025 - Added ForeCreate parameter for creating Batches and MoveRequests
    
.AUTHOR/COPYRIGHT:
    Steffen Meyer
    Cloud Solution Architect
    Microsoft Deutschland GmbH
#>

[CmdletBinding()]
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
     [Parameter(Mandatory=$false,Position=2,HelpMessage='Insert BadItemLimit value for move requests (e.g. 10, default = 0)')]
     [Switch]$ForceCreate,
     [Parameter(Mandatory=$false,Position=3,HelpMessage='Only with -ForceCreate, Batches and MoveRequests will be created')]
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

$version = "V1.3_21.10.2025"

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
                    Write-Host "`n We couldn't get a complete mailbox list for $($SourceDB)." -ForegroundColor Red
                    Exit 1
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
                    Write-Host "`n We couldn't get a complete PublicFolder mailbox list for $($SourceDB)." -ForegroundColor Red
                    Exit 1
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
                    Write-Host "`n We couldn't get a complete arbitration mailbox list for $($SourceDB)." -ForegroundColor Red
                    Exit 1
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
                    Write-Host "`n We couldn't get a complete AuditLog mailbox list for $($SourceDB)." -ForegroundColor Red
                    Exit 1
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
        Write-Host " NOTICE: Fetching all ARCHIVE mailboxes in Database $($SourceDB), this may take a while..." -ForegroundColor Yellow

        try
        {
            $Archives = Invoke-Expression "Get-Mailbox -Archive -resultsize unlimited -ignoredefaultscope -ErrorAction Stop -WarningAction SilentlyContinue | where-object Archivedatabase -like $SourceDB"
        }
        catch
        {
            Write-Host "`n We couldn't get a complete Archive mailbox list for $($SourceDB)." -ForegroundColor Red
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

#Start script
try
{
    $ScriptPath = Split-Path -Parent $MyInvocation.MyCommand.Path -ErrorAction Stop
}
catch
{
    Write-Host "`n Do not forget to save the script!" -ForegroundColor Red
}

Write-Host "`n Scriptversion: $version"
Write-Host   " Script started: $now   "

Write-Host "`n---------------------------------------------------------------------------------------" -Foregroundcolor Green
Write-Host   " Script to empty a too large or overloaded Single or a list of several Source DBs,     " -Foregroundcolor Green
Write-Host   " find destination databases for all mailboxes, archives and special mailboxes in a     " -Foregroundcolor Green
Write-Host   " CSV file and create migration batch(es) and move requests.                            " -Foregroundcolor Green
Write-Host "`n The script makes sure to distribute all mailboxes to be moved evenly and coordinated  " -ForegroundColor Green
Write-Host   " into staging databases. After the script you simply start and complete all migration  " -ForegroundColor Green
Write-Host   " batches and MoveRequests and you can simply delete the overloaded source databases    " -ForegroundColor Green
Write-Host   " and create an empty one. You will get empty space in both, DB and Volume, immediately." -ForegroundColor Green
Write-Host   "---------------------------------------------------------------------------------------" -Foregroundcolor Green

If (!($ForceCreate))
{
    Write-Host "`n-------------------------------------------------------------------------------------" -Foregroundcolor Yellow
    Write-Host   " ATTENTION: You started the script without ""-ForceCreate"" parameter. This means,   " -ForegroundColor Yellow
    Write-Host   " the script will not create any Batches or MoveRequests. It will create CSV files for" -ForegroundColor Yellow
    Write-Host   " Batches for review and will tell you the number of Batches and MoveRequests it      " -ForegroundColor Yellow
    Write-Host   " would create. If all is fine, just restart script with ""-ForceCreate"".            " -ForegroundColor Yellow
    Write-Host   "-------------------------------------------------------------------------------------" -Foregroundcolor Yellow
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
        Write-Host "`n The required source databases file $($SourceDBsCSV) file is missing. Add the file with a list of source DB(s) to ensure a working SCRIPT." -ForegroundColor Red
        Exit 1
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
    Write-Host "`n The required staging databases file $($StagingDBsCSV) file is missing. Add the file with a list of staging DB(s) to ensure a working SCRIPT." -ForegroundColor Red
    Exit 1
}

if (($SourceDBs | Where-Object {$StagingDBs -contains $_ }).count -gt 0)
{
    Write-Host "`n ATTENTION: We found same DB name(s) in both, Source AND Staging, that doesn't make sense, please verify, correct and restart script!" -ForegroundColor Red
    Exit 1
}

#Count Source- and StagingDBs
if ($SourceDBs -and $StagingDBs)
{
    $SourceDBsCount = ($SourceDBs).count
    $StagingDBsCount = ($StagingDBs).count
    
    Write-Host "`n Distribution of ALL mailboxes of $SourceDBsCount Source Database(s) into $StagingDBsCount Staging/Destination Database(s) evenly (by MailboxCount AND MailboxSizes)." -ForegroundColor Green
}

#If empty CSVs are found, exit the script
else
{
    if (!($SourceDBs))
    {
        Write-Host "`n ATTENTION: We found no Source Database(s), please verify content of SourceDBs-CSV and restart script!" -ForegroundColor Red
        Exit 1
    }
    if (!($StagingDBs))
    {
        Write-Host "`n ATTENTION: We found no Staging Database(s), please verify content of StagingDBs-CSV and restart script!" -ForegroundColor Red
        Exit 1
    }
}

#Check if Exchange SnapIn is available and load it
if (!(Get-PSSession).ConfigurationName -eq "Microsoft.Exchange")
{
    if ((Get-PSSnapin -Registered).name -contains "Microsoft.Exchange.Management.PowerShell.SnapIn")
    {
        Write-Host "`n Loading the Exchange Powershell SnapIn..." -ForegroundColor Yellow
        Add-PSSnapin Microsoft.Exchange.Management.PowerShell.SnapIn -ErrorAction SilentlyContinue
        . $env:ExchangeInstallPath\bin\RemoteExchange.ps1
        Connect-ExchangeServer -auto -AllowClobber
    }
    else
    {
        Write-Host "`n Exchange Management Tools are not installed. Run the script on a different machine." -ForegroundColor Red
        Return
    }
}

#Detect, where the script is executed
if (!(Get-ExchangeServer -Identity $env:COMPUTERNAME -ErrorAction SilentlyContinue))
{
    Write-Host "`n ATTENTION: Script is executed on a non-Exchangeserver..." -ForegroundColor Cyan
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
    Write-Host "`n NOTICE: Getting Mailbox Numbers for Database $($SourceDB)..." -ForegroundColor Cyan
    
    if ($NoNewProvisioning -and $ForceCreate)
    {
        $IsExcluded = Get-MailboxDatabase $SourceDB | Set-MailboxDatabase -IsExcludedFromProvisioning $True
        Write-Host " NOTICE: Source Database $($SourceDB) was excluded from further provisioning successfully."
    }
    $Mailboxes += Get-ExDBStatistics -SourceDB $SourceDB -MBXType ""

    $PFMBXs += Get-ExDBStatistics -SourceDB $SourceDB -MBXType "PublicFolder"

    $ArbitrationMBXs += Get-ExDBStatistics -SourceDB $SourceDB -MBXType "Arbitration"

    $AuditLogMBXs += Get-ExDBStatistics -SourceDB $SourceDB -MBXType "Auditlog"

    $ArchiveMBXs += Get-ExDBArchives -SourceDB $SourceDB
}

#Summarize mailboxcount to be moved
Write-Host "`n In your source database(s), we found the following numbers:" -ForegroundColor Green
Write-Host   " $($Mailboxes.Count) Standard mailbox(es)"
Write-Host   " $($ArchiveMBXs.Count) Archive mailbox(es)"
Write-Host   " $($PFMBXs.Count) PublicFolder mailbox(es)"
Write-Host   " $($ArbitrationMBXs.Count) Arbitration mailbox(es)"
Write-Host   " $($AuditLogMBXs.Count) Auditlog mailbox(es)"

#Fetch all MailboxStatistics of all Standard mailboxes of SourceDB(s) into an object sorted by ascending totalsize
if ($Mailboxes)
{
    $MailboxesResult = Get-ExMBXStatistics -Mailboxes $Mailboxes | Sort-Object SUM
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
    $ArchiveMBXsResult = Get-ExMBXStatistics -Mailboxes $ArchiveMBXs -Archive | Sort-Object ArchiveSUM
}

#Create an object for a CSV import file for MigrationBatch(es) for Standard mailboxes with a fitting staging database entry (by a simple algorithm schema)
$Index = 0
$Direction = 1
$MaxIndex = $StagingDBsCount - 1

$MailboxesBatchObject = foreach ($MailboxResult in $MailboxesResult)
{
    $TargetDatabase = $StagingDBs[$Index]

    [PSCustomObject]@{
    EmailAddress = $MailboxResult.EmailAddress
    TargetDatabase = $TargetDatabase
    BadItemLimit = $BadItemLimit
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

#Export to CSV file, every CSV will not consist of more than $MailboxBatchBlocksize mailboxes
$MailboxBatchCounter = 1

$MailboxesBatchObject | ForEach-Object -Begin {
    $Index = 0
    $Batch = @()
} -Process {
    $Batch += $_
    $Index++

    #always $MailboxBatchBlockSize mailboxes per batch
    if ($Index -eq $MailboxBatchBlocksize)
    {
        Write-Host "`n NOTICE: Creating Standard Mailboxes MigrationBatch $($MailboxBatchCounter)..." -ForegroundColor Cyan
        $Batch | Export-Csv -Path "$Scriptpath\batch_mailboxes_$($MailboxBatchCounter).csv" -NoTypeInformation -Encoding UTF8
        
        if ($ForceCreate)
        {        
            try
            {
                if ($EmailAddress)
                {
                    $MailboxMigrationBatch = New-MigrationBatch -Name "Batch_Mailboxes_$($MailboxBatchCounter)" -Local -CSVData ([System.IO.File]::ReadAllBytes("$Scriptpath\batch_mailboxes_$($MailboxBatchCounter).csv")) -PrimaryOnly -NotificationEmails $EmailAddress -ErrorAction Stop
                    Write-Host " MigrationBatch ""$($MailboxMigrationBatch.Identity)"" with $Index Standard mailbox(es) created successfully, Batch notifications will be sent to ""$EmailAddress""." -ForegroundColor Green
                }
                else
                {
                    $MailboxMigrationBatch = New-MigrationBatch -Name "Batch_Mailboxes_$($MailboxBatchCounter)" -Local -CSVData ([System.IO.File]::ReadAllBytes("$Scriptpath\batch_mailboxes_$($MailboxBatchCounter).csv")) -PrimaryOnly -ErrorAction Stop
                    Write-Host " MigrationBatch ""$($MailboxMigrationBatch.Identity)"" with $Index Standard mailbox(es) created successfully." -ForegroundColor Green
                }
            }
            catch
            {
                Write-Host " Couldn't create MigrationBatch ""$($MailboxMigrationBatch.Identity)"" for Standard mailboxes." -ForegroundColor Red
            }
        }
        $MailboxBatchCounter++
        $Batch = @()
        $Index = 0
        
    }
#put remaining mailboxes into final batch
} -End {
    if ($Batch.Count -gt 0) {
        Write-Host "`n NOTICE: Creating Standard Mailboxes MigrationBatch $($MailboxBatchCounter)..." -ForegroundColor Cyan
        $Batch | Export-Csv -Path "$Scriptpath\batch_mailboxes_$($MailboxBatchCounter).csv" -NoTypeInformation -Encoding UTF8
            
        if ($ForceCreate)
        {
            try
            {
                if ($EmailAddress)
                {
                    $MailboxMigrationBatch = New-MigrationBatch -Name "Batch_Mailboxes_$($MailboxBatchCounter)" -Local -CSVData ([System.IO.File]::ReadAllBytes("$Scriptpath\batch_mailboxes_$($MailboxBatchCounter).csv")) -PrimaryOnly -NotificationEmails $EmailAddress -ErrorAction Stop
                    Write-Host " MigrationBatch ""$($MailboxMigrationBatch.Identity)"" with $Index Standard mailbox(es) created successfully, Batch notifications will be sent to ""$EmailAddress""." -ForegroundColor Green
                }
                else
                {
                    $MailboxMigrationBatch = New-MigrationBatch -Name "Batch_Mailboxes_$($MailboxBatchCounter)" -Local -CSVData ([System.IO.File]::ReadAllBytes("$Scriptpath\batch_mailboxes_$($MailboxBatchCounter).csv")) -PrimaryOnly -ErrorAction Stop
                    Write-Host " MigrationBatch ""$($MailboxMigrationBatch.Identity)"" with $Index Standard mailbox(es) created successfully." -ForegroundColor Green
                }
            }
            catch
            {
                Write-Host " Couldn't create MigrationBatch ""$($MailboxMigrationBatch.Identity)"" for Standard mailboxes." -ForegroundColor Red
            }
        }
    }
}

#Create an object for a CSV import file for MigrationBatch(es) for Archive mailboxes with a fitting staging database entry (by a simple algorithm schema)
$StagingDBs = $StagingDBs | Sort-Object -Descending
$Index = 0
$Direction = 1
$MaxIndex = $StagingDBsCount - 1

$ArchiveMBXsBatchObject = foreach ($ArchiveMBXResult in $ArchiveMBXsResult)
{
$TargetArchiveDatabase = $StagingDBs[$Index]

    [PSCustomObject]@{
    EmailAddress = $ArchiveMBXResult.EmailAddress
    TargetArchiveDatabase = $TargetArchiveDatabase
    BadItemLimit = $BadItemLimit
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

#Export to CSV file, every CSV will not consist of more than $ArchiveBatchBlocksize mailboxes
$ArchiveBatchCounter = 1

$ArchiveMBXsBatchObject | ForEach-Object -Begin {
    $Index = 0
    $Batch = @()
} -Process {
    $Batch += $_
    $Index++

#always $ArchiveBatchBlockSize Archive mailboxes per batch
    if ($Index -eq $ArchiveBatchBlocksize)
    {
        Write-Host "`n NOTICE: Creating Archive Mailboxes MigrationBatch $($ArchiveBatchCounter)..." -ForegroundColor Cyan
        $Batch | Export-Csv -Path "$Scriptpath\batch_archives_$($ArchiveBatchCounter).csv" -NoTypeInformation -Encoding UTF8
        
        if ($ForceCreate)
        {
            try
            {
                if ($EmailAddress)
                {
                    $ArchiveMigrationBatch = New-MigrationBatch -Name "Batch_Archives_$($ArchiveBatchCounter)" -Local -CSVData ([System.IO.File]::ReadAllBytes("$Scriptpath\batch_archives_$($ArchiveBatchCounter).csv")) -ArchiveOnly -NotificationEmails $EmailAddress -ErrorAction Stop
                    Write-Host " MigrationBatch ""$($ArchiveMigrationBatch.Identity)"" with $Index Archive mailbox(es) created successfully, Batch notifications will be sent to ""$EmailAddress""." -ForegroundColor Green
                }
                else
                {
                    $ArchiveMigrationBatch = New-MigrationBatch -Name "Batch_Archives_$($ArchiveBatchCounter)" -Local -CSVData ([System.IO.File]::ReadAllBytes("$Scriptpath\batch_archives_$($ArchiveBatchCounter).csv")) -ArchiveOnly -ErrorAction Stop
                    Write-Host " MigrationBatch ""$($ArchiveMigrationBatch.Identity)"" with $Index Archive mailbox(es) created successfully" -ForegroundColor Green
                }
            }
            catch
            {
                Write-Host " Couldn't create MigrationBatch ""$($ArchiveMigrationBatch.Identity)"" for Archive mailboxes." -ForegroundColor Red
            }
        }
        $ArchiveBatchCounter++
        $Batch = @()
        $Index = 0
    }
#put remaining Archive mailboxes into final batch
} -End {
    if ($Batch.Count -gt 0) {
        Write-Host "`n NOTICE: Creating Archive Mailboxes MigrationBatch $($ArchiveBatchCounter)..." -ForegroundColor Cyan
        $Batch | Export-Csv -Path "$Scriptpath\batch_archives_$($ArchiveBatchCounter).csv" -NoTypeInformation -Encoding UTF8
        
        if ($ForceCreate)
        {
            try
            {
                if ($EmailAddress)
                {
                    $ArchiveMigrationBatch = New-MigrationBatch -Name "Batch_Archives_$($ArchiveBatchCounter)" -Local -CSVData ([System.IO.File]::ReadAllBytes("$Scriptpath\batch_archives_$($ArchiveBatchCounter).csv")) -ArchiveOnly -NotificationEmails $EmailAddress -ErrorAction Stop
                    Write-Host " MigrationBatch ""$($ArchiveMigrationBatch.Identity)"" with $Index Archive mailbox(es) created successfully, Batch notifications will be sent to ""$EmailAddress""." -ForegroundColor Green
                }
                else
                {
                    $ArchiveMigrationBatch = New-MigrationBatch -Name "Batch_Archives_$($ArchiveBatchCounter)" -Local -CSVData ([System.IO.File]::ReadAllBytes("$Scriptpath\batch_archives_$($ArchiveBatchCounter).csv")) -ArchiveOnly -ErrorAction Stop
                    Write-Host " MigrationBatch ""$($ArchiveMigrationBatch.Identity)"" with $Index Archive mailbox(es) created successfully." -ForegroundColor Green
                }
            }
            catch
            {
                Write-Host " Couldn't create MigrationBatch ""$($ArchiveMigrationBatch.Identity)"" for Archive mailboxes." -ForegroundColor Red
            }
        }
    }
}

#Create Move Requests for all PublicFolder Mailboxes in $SourceDB(s), StagingDatabase will be picked up from $StagingDBs randomly
$PFMoveRequestsCount = 0
if ($PFMBXs)
{
    ForEach ($PFMBX in $PFMBXs)
    {
        if ($ForceCreate)
        {
            Write-Host "`n NOTICE: Creating MoveRequest for PublicFolder Mailbox ""$($PFMBX)""..." -ForegroundColor Cyan
            $StagingDB = Get-Random -InputObject $StagingDBs
            $MoveRequest = Get-Mailbox -PublicFolder $PFMBX | New-MoveRequest -TargetDatabase $StagingDB -Suspend:$true -BadItemLimit $BadItemLimit -WarningAction SilentlyContinue
            Write-Host " Move Request ""$($MoveRequest.displayname)"" for PublicFolder mailbox ""$($PFMBX)"" created successfully." -ForegroundColor Green
        }
        $PFMoveRequestsCount++
    }
}

#Create Move Requests for all Arbitration mailboxes in $SourceDB(s), StagingDatabase will be picked up from $StagingDBs randomly
$ArbitrationMoveRequestsCount = 0
if ($ArbitrationMBXs)
{
    ForEach ($ArbitrationMBX in $ArbitrationMBXs)
    {
        if ($ForceCreate)
        {
            Write-Host "`n NOTICE: Creating MoveRequest for Arbitration Mailbox ""$($ArbitrationMBX)""..." -ForegroundColor Cyan
            $StagingDB = Get-Random -InputObject $StagingDBs
            $MoveRequest = Get-Mailbox -Arbitration $ArbitrationMBX | New-MoveRequest -TargetDatabase $StagingDB -Suspend:$true -BadItemLimit $BadItemLimit -WarningAction SilentlyContinue
            Write-Host " Move Request ""$($MoveRequest.displayname)"" for Arbitration mailbox ""$($ArbitrationMBX)"" created successfully." -ForegroundColor Green
        }
        $ArbitrationMoveRequestsCount++
    }
}

#Create Move Requests for all Auditlog mailboxes in $SourceDB(s), StagingDatabase will be picked up from $StagingDBs randomly
$AuditLogMoveRequestsCount = 0
if ($AuditlogMBXs)
{
    ForEach ($AuditlogMBX in $AuditlogMBXs)
    {
        if ($ForceCreate)
        {
            Write-Host "`n NOTICE: Creating MoveRequest for AuditLog Mailbox ""$($AuditLogMBX)""..." -ForegroundColor Cyan
            $StagingDB = Get-Random -InputObject $StagingDBs
            $MoveRequest = Get-Mailbox -AuditLog $AuditlogMBX | New-MoveRequest -TargetDatabase $StagingDB -Suspend:$true -BadItemLimit $BadItemLimit -WarningAction SilentlyContinue
            Write-Host " Move Request ""$($MoveRequest.displayname)"" for Auditlog mailbox ""$($AuditlogMBX)"" created successfully." -ForegroundColor Green
        }
        $AuditLogMoveRequestsCount++
    }
}

#Final statement for manual steps to follow for empty and re-create Source Database(s)
Write-Host "`n PLEASE READ CAREFULLY:" -ForegroundColor Yellow
Write-Host   "------------------------------------------------------------------------------------------------------"
if ($ForceCreate) {Write-Host " This script created:"} else {Write-Host " This script would create:"}
Write-Host "`n $($MailboxBatchCounter) MigrationBatch(es) for Standard mailboxes,                              " -ForegroundColor Cyan
Write-Host   " $($ArchiveBatchCounter) MigrationBatch(es) for Archive mailboxes,                               " -ForegroundColor Cyan
Write-Host   " $($PFMoveRequestsCount) MoveRequest(s) for PublicFolder mailboxes,                              " -ForegroundColor Cyan
Write-Host   " $($ArbitrationMoveRequestsCount) MoveRequest(s) for Arbitration mailboxes,                      " -ForegroundColor Cyan
Write-Host   " $($AuditLogMoveRequestsCount) MoveRequest(s) for Auditlog mailboxes                             " -ForegroundColor Cyan
Write-Host "`n of the selected SourceDatabase(s).                                                              "
if ($ForceCreate)
{
    Write-Host "`n You need to start AND complete the batches MANUALLY and you need to resume/start the            " -ForegroundColor Yellow
    Write-Host   " MoveRequests MANUALLY, (ATTENTION!) MoveRequests will be completed automatically.               " -ForegroundColor Yellow
    Write-Host "`n After it, the selected Source database(s) is/are empty and the EDB files can be safely          "
    Write-Host   " deleted to free up space in the volume and to reduce the size of the database files without     "
    Write-Host   " using legacy offline database defragmention.                                                    "
    Write-Host   " Do not forget to initial re-seed all copies and, if legacy Exchange backup is in place, to take "
    Write-Host   " a FULL BACKUP after Database re-creation immediately.                                           "
    Write-Host   "-------------------------------------------------------------------------------------------------"
}
else
{
    Write-Host "`n If you want to create Batches and MoveRequests, just restart the script with parameter ""-ForceCreate""" -ForegroundColor Yellow
}
#END