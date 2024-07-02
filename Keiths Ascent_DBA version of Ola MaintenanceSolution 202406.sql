/*
Updated: 2024-06-14

This is based on:					   

SQL Server Maintenance Solution - SQL Server 2008, SQL Server 2008 R2, SQL Server 2012, SQL Server 2014, SQL Server 2016, SQL Server 2017, and SQL Server 2019

Version: 2022-12-03 17:23:44

Ola Hallengren
https://ola.hallengren.com

-- added sp_DatabaseRestore from the Brent Ozar First Responder Kit
-- Version = '8.20', VersionDate = '20240522'

Added Who Is Active? v12.00 (2021-11-10)
(C) 2007-2020, Adam Machanic
http://whoisactive.com

*/

USE [master] -- Specify the database in which the objects will be created.

IF NOT EXISTS(SELECT NAME FROM SYSDATABASES WHERE NAME = 'Ascent_DBA')
BEGIN
CREATE DATABASE [Ascent_DBA]
END
GO
			 
USE [Ascent_DBA] -- Specify the database in which the objects will be created.

SET NOCOUNT ON

DECLARE @CreateJobs nvarchar(max)          = 'Y'         -- Specify whether jobs should be created.
DECLARE @BackupDirectory nvarchar(max)     = 'F:\SQLBackups\'        -- Specify the backup root directory. If no directory is specified, the default backup directory is used.
DECLARE @CleanupTime int                   = 168        -- Time in hours, after which backup files are deleted. (168 = 7 days.) If no time is specified, then no backup files are deleted.
DECLARE @OutputFileDirectory nvarchar(max) = NULL        -- Specify the output file directory. If no directory is specified, then the SQL Server error log directory is used.
DECLARE @LogToTable nvarchar(max)          = 'Y'         -- Log commands to a table.

DECLARE @ErrorMessage nvarchar(max)

-- Ascent
DECLARE @CreateBackupJobs nvarchar(1)
DECLARE @CreateIndexJobs nvarchar(1)
DECLARE @CreateDBCCJobs nvarchar(1)
DECLARE @CreateJobSchedules nvarchar(1)
DECLARE @ExistingJobsAction nvarchar(10) = 'UPDATE'			--Either 'UPDATE', 'REPLACE' OR 'NONE'

SET @CreateBackupJobs    = 'Y'          -- Specify whether Backup jobs should be created.
SET @CreateIndexJobs     = 'Y'          -- Specify whether Index Maintenance jobs should be created.
SET @CreateDBCCJobs      = 'Y'          -- Specify whether Integrity Check jobs should be created.
SET @CreateJobSchedules	 = 'Y'			-- Specify whether standard job schedules should be created.
SET @ExistingJobsAction  = 'UPDATE'		-- Specify what to do if Jobs already exist
-- \Ascent
IF IS_SRVROLEMEMBER('sysadmin') = 0 AND NOT (DB_ID('rdsadmin') IS NOT NULL AND SUSER_SNAME(0x01) = 'rdsa')
BEGIN
  SET @ErrorMessage = 'You need to be a member of the SysAdmin server role to install the SQL Server Maintenance Solution.'
  RAISERROR(@ErrorMessage,16,1) WITH NOWAIT
END

IF NOT (SELECT [compatibility_level] FROM sys.databases WHERE database_id = DB_ID()) >= 90
BEGIN
  SET @ErrorMessage = 'The database ' + QUOTENAME(DB_NAME(DB_ID())) + ' has to be in compatibility level 90 or higher.'
  RAISERROR(@ErrorMessage,16,1) WITH NOWAIT
END

IF OBJECT_ID('tempdb..#Config') IS NOT NULL DROP TABLE #Config

CREATE TABLE #Config ([Name] nvarchar(max),
                      [Value] nvarchar(max))
-- Ascent
INSERT INTO #Config ([Name], [Value]) VALUES('CreateBackupJobs', @CreateBackupJobs)
INSERT INTO #Config ([Name], [Value]) VALUES('CreateIndexJobs', @CreateIndexJobs)
INSERT INTO #Config ([Name], [Value]) VALUES('CreateDBCCJobs', @CreateDBCCJobs)
INSERT INTO #Config ([Name], [Value]) VALUES('CreateJobSchedules', @CreateJobSchedules)
INSERT INTO #Config ([Name], [Value]) VALUES('ExistingJobsAction', @ExistingJobsAction)
-- \Ascent

INSERT INTO #Config ([Name], [Value]) VALUES('CreateJobs', @CreateJobs)
INSERT INTO #Config ([Name], [Value]) VALUES('BackupDirectory', @BackupDirectory)
INSERT INTO #Config ([Name], [Value]) VALUES('CleanupTime', @CleanupTime)
INSERT INTO #Config ([Name], [Value]) VALUES('OutputFileDirectory', @OutputFileDirectory)
INSERT INTO #Config ([Name], [Value]) VALUES('LogToTable', @LogToTable)
INSERT INTO #Config ([Name], [Value]) VALUES('DatabaseName', DB_NAME(DB_ID()))
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[CommandLog]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[CommandLog](
  [ID] [int] IDENTITY(1,1) NOT NULL,
  [DatabaseName] [sysname] NULL,
  [SchemaName] [sysname] NULL,
  [ObjectName] [sysname] NULL,
  [ObjectType] [char](2) NULL,
  [IndexName] [sysname] NULL,
  [IndexType] [tinyint] NULL,
  [StatisticsName] [sysname] NULL,
  [PartitionNumber] [int] NULL,
  [ExtendedInfo] [xml] NULL,
  [Command] [nvarchar](max) NOT NULL,
  [CommandType] [nvarchar](60) NOT NULL,
  [StartTime] [datetime2](7) NOT NULL,
  [EndTime] [datetime2](7) NULL,
  [ErrorNumber] [int] NULL,
  [ErrorMessage] [nvarchar](max) NULL,
 CONSTRAINT [PK_CommandLog] PRIMARY KEY CLUSTERED
(
  [ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
)
END
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[CommandExecute]') AND type in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [dbo].[CommandExecute] AS'
END
GO
ALTER PROCEDURE [dbo].[CommandExecute]

@DatabaseContext nvarchar(max),
@Command nvarchar(max),
@CommandType nvarchar(max),
@Mode int,
@Comment nvarchar(max) = NULL,
@DatabaseName nvarchar(max) = NULL,
@SchemaName nvarchar(max) = NULL,
@ObjectName nvarchar(max) = NULL,
@ObjectType nvarchar(max) = NULL,
@IndexName nvarchar(max) = NULL,
@IndexType int = NULL,
@StatisticsName nvarchar(max) = NULL,
@PartitionNumber int = NULL,
@ExtendedInfo xml = NULL,
@LockMessageSeverity int = 16,
@ExecuteAsUser nvarchar(max) = NULL,
@LogToTable nvarchar(max),
@Execute nvarchar(max)

AS

BEGIN

  ----------------------------------------------------------------------------------------------------
  --// Source:  Database Maintenance Solution provided by Ascent Technology PTY (Ltd) //--
  --// https://www.ascent.tech  //--
  --// Version: 2022-12-03 17:23:44                                                              //--
  ----------------------------------------------------------------------------------------------------

  SET NOCOUNT ON

  DECLARE @StartMessage nvarchar(max)
  DECLARE @EndMessage nvarchar(max)
  DECLARE @ErrorMessage nvarchar(max)
  DECLARE @ErrorMessageOriginal nvarchar(max)
  DECLARE @Severity int

  DECLARE @Errors TABLE (ID int IDENTITY PRIMARY KEY,
                         [Message] nvarchar(max) NOT NULL,
                         Severity int NOT NULL,
                         [State] int)

  DECLARE @CurrentMessage nvarchar(max)
  DECLARE @CurrentSeverity int
  DECLARE @CurrentState int

  DECLARE @sp_executesql nvarchar(max) = QUOTENAME(@DatabaseContext) + '.sys.sp_executesql'

  DECLARE @StartTime datetime2
  DECLARE @EndTime datetime2

  DECLARE @ID int

  DECLARE @Error int = 0
  DECLARE @ReturnCode int = 0

  DECLARE @EmptyLine nvarchar(max) = CHAR(9)

  DECLARE @RevertCommand nvarchar(max)

  ----------------------------------------------------------------------------------------------------
  --// Check core requirements                                                                    //--
  ----------------------------------------------------------------------------------------------------

  IF NOT (SELECT [compatibility_level] FROM sys.databases WHERE database_id = DB_ID()) >= 90
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The database ' + QUOTENAME(DB_NAME(DB_ID())) + ' has to be in compatibility level 90 or higher.', 16, 1
  END

  IF NOT (SELECT uses_ansi_nulls FROM sys.sql_modules WHERE [object_id] = @@PROCID) = 1
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'ANSI_NULLS has to be set to ON for the stored procedure.', 16, 1
  END

  IF NOT (SELECT uses_quoted_identifier FROM sys.sql_modules WHERE [object_id] = @@PROCID) = 1
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'QUOTED_IDENTIFIER has to be set to ON for the stored procedure.', 16, 1
  END

  IF @LogToTable = 'Y' AND NOT EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'U' AND schemas.[name] = 'dbo' AND objects.[name] = 'CommandLog')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The table CommandLog is missing. Contact your Ascent Technology Consultant. https://www.ascent.tech ', 16, 1
  END

  ----------------------------------------------------------------------------------------------------
  --// Check input parameters                                                                     //--
  ----------------------------------------------------------------------------------------------------

  IF @DatabaseContext IS NULL OR NOT EXISTS (SELECT * FROM sys.databases WHERE name = @DatabaseContext)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @DatabaseContext is not supported.', 16, 1
  END

  IF @Command IS NULL OR @Command = ''
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Command is not supported.', 16, 1
  END

  IF @CommandType IS NULL OR @CommandType = '' OR LEN(@CommandType) > 60
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @CommandType is not supported.', 16, 1
  END

  IF @Mode NOT IN(1,2) OR @Mode IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Mode is not supported.', 16, 1
  END

  IF @LockMessageSeverity NOT IN(10,16) OR @LockMessageSeverity IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @LockMessageSeverity is not supported.', 16, 1
  END

  IF LEN(@ExecuteAsUser) > 128
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @ExecuteAsUser is not supported.', 16, 1
  END

  IF @LogToTable NOT IN('Y','N') OR @LogToTable IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @LogToTable is not supported.', 16, 1
  END

  IF @Execute NOT IN('Y','N') OR @Execute IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Execute is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------
  --// Raise errors                                                                               //--
  ----------------------------------------------------------------------------------------------------

  DECLARE ErrorCursor CURSOR FAST_FORWARD FOR SELECT [Message], Severity, [State] FROM @Errors ORDER BY [ID] ASC

  OPEN ErrorCursor

  FETCH ErrorCursor INTO @CurrentMessage, @CurrentSeverity, @CurrentState

  WHILE @@FETCH_STATUS = 0
  BEGIN
    RAISERROR('%s', @CurrentSeverity, @CurrentState, @CurrentMessage) WITH NOWAIT
    RAISERROR(@EmptyLine, 10, 1) WITH NOWAIT

    FETCH NEXT FROM ErrorCursor INTO @CurrentMessage, @CurrentSeverity, @CurrentState
  END

  CLOSE ErrorCursor

  DEALLOCATE ErrorCursor

  IF EXISTS (SELECT * FROM @Errors WHERE Severity >= 16)
  BEGIN
    SET @ReturnCode = 50000
    GOTO ReturnCode
  END

  ----------------------------------------------------------------------------------------------------
  --// Execute as user                                                                            //--
  ----------------------------------------------------------------------------------------------------

  IF @ExecuteAsUser IS NOT NULL
  BEGIN
    SET @Command = 'EXECUTE AS USER = ''' + REPLACE(@ExecuteAsUser,'''','''''') + '''; ' + @Command + '; REVERT;'

    SET @RevertCommand = 'REVERT'
  END

  ----------------------------------------------------------------------------------------------------
  --// Log initial information                                                                    //--
  ----------------------------------------------------------------------------------------------------

  SET @StartTime = SYSDATETIME()

  SET @StartMessage = 'Date and time: ' + CONVERT(nvarchar,@StartTime,120)
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Database context: ' + QUOTENAME(@DatabaseContext)
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Command: ' + @Command
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  IF @Comment IS NOT NULL
  BEGIN
    SET @StartMessage = 'Comment: ' + @Comment
    RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT
  END

  IF @LogToTable = 'Y'
  BEGIN
    INSERT INTO dbo.CommandLog (DatabaseName, SchemaName, ObjectName, ObjectType, IndexName, IndexType, StatisticsName, PartitionNumber, ExtendedInfo, CommandType, Command, StartTime)
    VALUES (@DatabaseName, @SchemaName, @ObjectName, @ObjectType, @IndexName, @IndexType, @StatisticsName, @PartitionNumber, @ExtendedInfo, @CommandType, @Command, @StartTime)
  END

  SET @ID = SCOPE_IDENTITY()

  ----------------------------------------------------------------------------------------------------
  --// Execute command                                                                            //--
  ----------------------------------------------------------------------------------------------------

  IF @Mode = 1 AND @Execute = 'Y'
  BEGIN
    EXECUTE @sp_executesql @stmt = @Command
    SET @Error = @@ERROR
    SET @ReturnCode = @Error
  END

  IF @Mode = 2 AND @Execute = 'Y'
  BEGIN
    BEGIN TRY
      EXECUTE @sp_executesql @stmt = @Command
    END TRY
    BEGIN CATCH
      SET @Error = ERROR_NUMBER()
      SET @ErrorMessageOriginal = ERROR_MESSAGE()

      SET @ErrorMessage = 'Msg ' + CAST(ERROR_NUMBER() AS nvarchar) + ', ' + ISNULL(ERROR_MESSAGE(),'')
      SET @Severity = CASE WHEN ERROR_NUMBER() IN(1205,1222) THEN @LockMessageSeverity ELSE 16 END
      RAISERROR('%s',@Severity,1,@ErrorMessage) WITH NOWAIT

      IF NOT (ERROR_NUMBER() IN(1205,1222) AND @LockMessageSeverity = 10)
      BEGIN
        SET @ReturnCode = ERROR_NUMBER()
      END

      IF @ExecuteAsUser IS NOT NULL
      BEGIN
        EXECUTE @sp_executesql @RevertCommand
      END
    END CATCH
  END

  ----------------------------------------------------------------------------------------------------
  --// Log completing information                                                                 //--
  ----------------------------------------------------------------------------------------------------

  SET @EndTime = SYSDATETIME()

  SET @EndMessage = 'Outcome: ' + CASE WHEN @Execute = 'N' THEN 'Not Executed' WHEN @Error = 0 THEN 'Succeeded' ELSE 'Failed' END
  RAISERROR('%s',10,1,@EndMessage) WITH NOWAIT

  SET @EndMessage = 'Duration: ' + CASE WHEN (DATEDIFF(SECOND,@StartTime,@EndTime) / (24 * 3600)) > 0 THEN CAST((DATEDIFF(SECOND,@StartTime,@EndTime) / (24 * 3600)) AS nvarchar) + '.' ELSE '' END + CONVERT(nvarchar,DATEADD(SECOND,DATEDIFF(SECOND,@StartTime,@EndTime),'1900-01-01'),108)
  RAISERROR('%s',10,1,@EndMessage) WITH NOWAIT

  SET @EndMessage = 'Date and time: ' + CONVERT(nvarchar,@EndTime,120)
  RAISERROR('%s',10,1,@EndMessage) WITH NOWAIT

  RAISERROR(@EmptyLine,10,1) WITH NOWAIT

  IF @LogToTable = 'Y'
  BEGIN
    UPDATE dbo.CommandLog
    SET EndTime = @EndTime,
        ErrorNumber = CASE WHEN @Execute = 'N' THEN NULL ELSE @Error END,
        ErrorMessage = @ErrorMessageOriginal
    WHERE ID = @ID
  END

  ReturnCode:
  IF @ReturnCode <> 0
  BEGIN
    RETURN @ReturnCode
  END

  ----------------------------------------------------------------------------------------------------

END
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[DatabaseBackup]') AND type in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [dbo].[DatabaseBackup] AS'
END
GO
ALTER PROCEDURE [dbo].[DatabaseBackup]

@Databases nvarchar(max) = NULL,
@Directory nvarchar(max) = NULL,
@BackupType nvarchar(max),
@Verify nvarchar(max) = 'N',
@CleanupTime int = NULL,
@CleanupMode nvarchar(max) = 'AFTER_BACKUP',
@Compress nvarchar(max) = NULL,
@CopyOnly nvarchar(max) = 'N',
@ChangeBackupType nvarchar(max) = 'N',
@BackupSoftware nvarchar(max) = NULL,
@CheckSum nvarchar(max) = 'N',
@BlockSize int = NULL,
@BufferCount int = NULL,
@MaxTransferSize int = NULL,
@NumberOfFiles int = NULL,
@MinBackupSizeForMultipleFiles int = NULL,
@MaxFileSize int = NULL,
@CompressionLevel int = NULL,
@Description nvarchar(max) = NULL,
@Threads int = NULL,
@Throttle int = NULL,
@Encrypt nvarchar(max) = 'N',
@EncryptionAlgorithm nvarchar(max) = NULL,
@ServerCertificate nvarchar(max) = NULL,
@ServerAsymmetricKey nvarchar(max) = NULL,
@EncryptionKey nvarchar(max) = NULL,
@ReadWriteFileGroups nvarchar(max) = 'N',
@OverrideBackupPreference nvarchar(max) = 'N',
@NoRecovery nvarchar(max) = 'N',
@URL nvarchar(max) = NULL,
@Credential nvarchar(max) = NULL,
@MirrorDirectory nvarchar(max) = NULL,
@MirrorCleanupTime int = NULL,
@MirrorCleanupMode nvarchar(max) = 'AFTER_BACKUP',
@MirrorURL nvarchar(max) = NULL,
@AvailabilityGroups nvarchar(max) = NULL,
@Updateability nvarchar(max) = 'ALL',
@AdaptiveCompression nvarchar(max) = NULL,
@ModificationLevel int = NULL,
@LogSizeSinceLastLogBackup int = NULL,
@TimeSinceLastLogBackup int = NULL,
@DataDomainBoostHost nvarchar(max) = NULL,
@DataDomainBoostUser nvarchar(max) = NULL,
@DataDomainBoostDevicePath nvarchar(max) = NULL,
@DataDomainBoostLockboxPath nvarchar(max) = NULL,
@DirectoryStructure nvarchar(max) = '{ServerName}${InstanceName}{DirectorySeparator}{DatabaseName}{DirectorySeparator}{BackupType}_{Partial}_{CopyOnly}',
@AvailabilityGroupDirectoryStructure nvarchar(max) = '{ClusterName}${AvailabilityGroupName}{DirectorySeparator}{DatabaseName}{DirectorySeparator}{BackupType}_{Partial}_{CopyOnly}',
@FileName nvarchar(max) = '{ServerName}${InstanceName}_{DatabaseName}_{BackupType}_{Partial}_{CopyOnly}_{Year}{Month}{Day}_{Hour}{Minute}{Second}_{FileNumber}.{FileExtension}',
@AvailabilityGroupFileName nvarchar(max) = '{ClusterName}${AvailabilityGroupName}_{DatabaseName}_{BackupType}_{Partial}_{CopyOnly}_{Year}{Month}{Day}_{Hour}{Minute}{Second}_{FileNumber}.{FileExtension}',
@FileExtensionFull nvarchar(max) = NULL,
@FileExtensionDiff nvarchar(max) = NULL,
@FileExtensionLog nvarchar(max) = NULL,
@Init nvarchar(max) = 'N',
@Format nvarchar(max) = 'N',
@ObjectLevelRecoveryMap nvarchar(max) = 'N',
@ExcludeLogShippedFromLogBackup nvarchar(max) = 'Y',
@DirectoryCheck nvarchar(max) = 'Y',
@StringDelimiter nvarchar(max) = ',',
@DatabaseOrder nvarchar(max) = NULL,
@DatabasesInParallel nvarchar(max) = 'N',
@LogToTable nvarchar(max) = 'N',
@Execute nvarchar(max) = 'Y'

AS

BEGIN

  ----------------------------------------------------------------------------------------------------
  --// Source:  Database Maintenance Solution provided by Ascent Technology PTY (Ltd) //--							 
  --// https://www.ascent.tech  //--							 
  --// Version: 2022-01-02 13:58:13//--
  ----------------------------------------------------------------------------------------------------

  SET NOCOUNT ON

  DECLARE @StartMessage nvarchar(max)
  DECLARE @EndMessage nvarchar(max)
  DECLARE @DatabaseMessage nvarchar(max)
  DECLARE @ErrorMessage nvarchar(max)

  DECLARE @StartTime datetime2 = SYSDATETIME()
  DECLARE @SchemaName nvarchar(max) = OBJECT_SCHEMA_NAME(@@PROCID)
  DECLARE @ObjectName nvarchar(max) = OBJECT_NAME(@@PROCID)
  DECLARE @VersionTimestamp nvarchar(max) = SUBSTRING(OBJECT_DEFINITION(@@PROCID),CHARINDEX('--// Version: ',OBJECT_DEFINITION(@@PROCID)) + LEN('--// Version: ') + 1, 19)
  DECLARE @Parameters nvarchar(max)

  DECLARE @HostPlatform nvarchar(max)
  DECLARE @DirectorySeparator nvarchar(max)

  DECLARE @Updated bit

  DECLARE @Cluster nvarchar(max)

  DECLARE @DefaultDirectory nvarchar(4000)

  DECLARE @QueueID int
  DECLARE @QueueStartTime datetime2

  DECLARE @CurrentRootDirectoryID int
  DECLARE @CurrentRootDirectoryPath nvarchar(4000)

  DECLARE @CurrentDBID int
  DECLARE @CurrentDatabaseName nvarchar(max)

  DECLARE @CurrentDatabase_sp_executesql nvarchar(max)

  DECLARE @CurrentUserAccess nvarchar(max)
  DECLARE @CurrentIsReadOnly bit
  DECLARE @CurrentDatabaseState nvarchar(max)
  DECLARE @CurrentInStandby bit
  DECLARE @CurrentRecoveryModel nvarchar(max)
  DECLARE @CurrentDatabaseSize bigint

  DECLARE @CurrentIsEncrypted bit

  DECLARE @CurrentBackupType nvarchar(max)
  DECLARE @CurrentMaxTransferSize int
  DECLARE @CurrentNumberOfFiles int
  DECLARE @CurrentFileExtension nvarchar(max)
  DECLARE @CurrentFileNumber int
  DECLARE @CurrentDifferentialBaseLSN numeric(25,0)
  DECLARE @CurrentDifferentialBaseIsSnapshot bit
  DECLARE @CurrentLogLSN numeric(25,0)
  DECLARE @CurrentLatestBackup datetime2
  DECLARE @CurrentDatabaseNameFS nvarchar(max)
  DECLARE @CurrentDirectoryStructure nvarchar(max)
  DECLARE @CurrentDatabaseFileName nvarchar(max)
  DECLARE @CurrentMaxFilePathLength nvarchar(max)
  DECLARE @CurrentFileName nvarchar(max)
  DECLARE @CurrentDirectoryID int
  DECLARE @CurrentDirectoryPath nvarchar(max)
  DECLARE @CurrentFilePath nvarchar(max)
  DECLARE @CurrentDate datetime2
  DECLARE @CurrentCleanupDate datetime2
  DECLARE @CurrentIsDatabaseAccessible bit
  DECLARE @CurrentReplicaID uniqueidentifier
  DECLARE @CurrentAvailabilityGroupID uniqueidentifier
  DECLARE @CurrentAvailabilityGroup nvarchar(max)
  DECLARE @CurrentAvailabilityGroupRole nvarchar(max)
  DECLARE @CurrentAvailabilityGroupBackupPreference nvarchar(max)
  DECLARE @CurrentIsPreferredBackupReplica bit
  DECLARE @CurrentDatabaseMirroringRole nvarchar(max)
  DECLARE @CurrentLogShippingRole nvarchar(max)

  DECLARE @CurrentBackupSetID int
  DECLARE @CurrentIsMirror bit
  DECLARE @CurrentLastLogBackup datetime2
  DECLARE @CurrentLogSizeSinceLastLogBackup float
  DECLARE @CurrentAllocatedExtentPageCount bigint
  DECLARE @CurrentModifiedExtentPageCount bigint

  DECLARE @CurrentDatabaseContext nvarchar(max)
  DECLARE @CurrentCommand nvarchar(max)
  DECLARE @CurrentCommandOutput int
  DECLARE @CurrentCommandType nvarchar(max)

  DECLARE @Errors TABLE (ID int IDENTITY PRIMARY KEY,
                         [Message] nvarchar(max) NOT NULL,
                         Severity int NOT NULL,
                         [State] int)

  DECLARE @CurrentMessage nvarchar(max)
  DECLARE @CurrentSeverity int
  DECLARE @CurrentState int

  DECLARE @Directories TABLE (ID int PRIMARY KEY,
                              DirectoryPath nvarchar(max),
                              Mirror bit,
                              Completed bit)

  DECLARE @URLs TABLE (ID int PRIMARY KEY,
                       DirectoryPath nvarchar(max),
                       Mirror bit)

  DECLARE @DirectoryInfo TABLE (FileExists bit,
                                FileIsADirectory bit,
                                ParentDirectoryExists bit)

  DECLARE @tmpDatabases TABLE (ID int IDENTITY,
                               DatabaseName nvarchar(max),
                               DatabaseNameFS nvarchar(max),
                               DatabaseType nvarchar(max),
                               AvailabilityGroup bit,
                               StartPosition int,
                               DatabaseSize bigint,
                               LogSizeSinceLastLogBackup float,
                               [Order] int,
                               Selected bit,
                               Completed bit,
                               PRIMARY KEY(Selected, Completed, [Order], ID))

  DECLARE @tmpAvailabilityGroups TABLE (ID int IDENTITY PRIMARY KEY,
                                        AvailabilityGroupName nvarchar(max),
                                        StartPosition int,
                                        Selected bit)

  DECLARE @tmpDatabasesAvailabilityGroups TABLE (DatabaseName nvarchar(max),
                                                 AvailabilityGroupName nvarchar(max))

  DECLARE @SelectedDatabases TABLE (DatabaseName nvarchar(max),
                                    DatabaseType nvarchar(max),
                                    AvailabilityGroup nvarchar(max),
                                    StartPosition int,
                                    Selected bit)

  DECLARE @SelectedAvailabilityGroups TABLE (AvailabilityGroupName nvarchar(max),
                                             StartPosition int,
                                             Selected bit)

  DECLARE @CurrentBackupOutput bit

  DECLARE @CurrentBackupSet TABLE (ID int IDENTITY PRIMARY KEY,
                                   Mirror bit,
                                   VerifyCompleted bit,
                                   VerifyOutput int)

  DECLARE @CurrentDirectories TABLE (ID int PRIMARY KEY,
                                     DirectoryPath nvarchar(max),
                                     Mirror bit,
                                     DirectoryNumber int,
                                     CleanupDate datetime2,
                                     CleanupMode nvarchar(max),
                                     CreateCompleted bit,
                                     CleanupCompleted bit,
                                     CreateOutput int,
                                     CleanupOutput int)

  DECLARE @CurrentURLs TABLE (ID int PRIMARY KEY,
                              DirectoryPath nvarchar(max),
                              Mirror bit,
                              DirectoryNumber int)

  DECLARE @CurrentFiles TABLE ([Type] nvarchar(max),
                               FilePath nvarchar(max),
                               Mirror bit)

  DECLARE @CurrentCleanupDates TABLE (CleanupDate datetime2,
                                      Mirror bit)

  DECLARE @Error int = 0
  DECLARE @ReturnCode int = 0

  DECLARE @EmptyLine nvarchar(max) = CHAR(9)

  DECLARE @Version numeric(18,10) = CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max)),CHARINDEX('.',CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max))) - 1) + '.' + REPLACE(RIGHT(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max)), LEN(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max))) - CHARINDEX('.',CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max)))),'.','') AS numeric(18,10))

  IF @Version >= 14
  BEGIN
    SELECT @HostPlatform = host_platform
    FROM sys.dm_os_host_info
  END
  ELSE
  BEGIN
    SET @HostPlatform = 'Windows'
  END

  DECLARE @AmazonRDS bit = CASE WHEN DB_ID('rdsadmin') IS NOT NULL AND SUSER_SNAME(0x01) = 'rdsa' THEN 1 ELSE 0 END

  ----------------------------------------------------------------------------------------------------
  --// Log initial information                                                                    //--
  ----------------------------------------------------------------------------------------------------

  SET @Parameters = '@Databases = ' + ISNULL('''' + REPLACE(@Databases,'''','''''') + '''','NULL')
  SET @Parameters += ', @Directory = ' + ISNULL('''' + REPLACE(@Directory,'''','''''') + '''','NULL')
  SET @Parameters += ', @BackupType = ' + ISNULL('''' + REPLACE(@BackupType,'''','''''') + '''','NULL')
  SET @Parameters += ', @Verify = ' + ISNULL('''' + REPLACE(@Verify,'''','''''') + '''','NULL')
  SET @Parameters += ', @CleanupTime = ' + ISNULL(CAST(@CleanupTime AS nvarchar),'NULL')
  SET @Parameters += ', @CleanupMode = ' + ISNULL('''' + REPLACE(@CleanupMode,'''','''''') + '''','NULL')
  SET @Parameters += ', @Compress = ' + ISNULL('''' + REPLACE(@Compress,'''','''''') + '''','NULL')
  SET @Parameters += ', @CopyOnly = ' + ISNULL('''' + REPLACE(@CopyOnly,'''','''''') + '''','NULL')
  SET @Parameters += ', @ChangeBackupType = ' + ISNULL('''' + REPLACE(@ChangeBackupType,'''','''''') + '''','NULL')
  SET @Parameters += ', @BackupSoftware = ' + ISNULL('''' + REPLACE(@BackupSoftware,'''','''''') + '''','NULL')
  SET @Parameters += ', @CheckSum = ' + ISNULL('''' + REPLACE(@CheckSum,'''','''''') + '''','NULL')
  SET @Parameters += ', @BlockSize = ' + ISNULL(CAST(@BlockSize AS nvarchar),'NULL')
  SET @Parameters += ', @BufferCount = ' + ISNULL(CAST(@BufferCount AS nvarchar),'NULL')
  SET @Parameters += ', @MaxTransferSize = ' + ISNULL(CAST(@MaxTransferSize AS nvarchar),'NULL')
  SET @Parameters += ', @NumberOfFiles = ' + ISNULL(CAST(@NumberOfFiles AS nvarchar),'NULL')
  SET @Parameters += ', @MinBackupSizeForMultipleFiles = ' + ISNULL(CAST(@MinBackupSizeForMultipleFiles AS nvarchar),'NULL')
  SET @Parameters += ', @MaxFileSize = ' + ISNULL(CAST(@MaxFileSize AS nvarchar),'NULL')
  SET @Parameters += ', @CompressionLevel = ' + ISNULL(CAST(@CompressionLevel AS nvarchar),'NULL')
  SET @Parameters += ', @Description = ' + ISNULL('''' + REPLACE(@Description,'''','''''') + '''','NULL')
  SET @Parameters += ', @Threads = ' + ISNULL(CAST(@Threads AS nvarchar),'NULL')
  SET @Parameters += ', @Throttle = ' + ISNULL(CAST(@Throttle AS nvarchar),'NULL')
  SET @Parameters += ', @Encrypt = ' + ISNULL('''' + REPLACE(@Encrypt,'''','''''') + '''','NULL')
  SET @Parameters += ', @EncryptionAlgorithm = ' + ISNULL('''' + REPLACE(@EncryptionAlgorithm,'''','''''') + '''','NULL')
  SET @Parameters += ', @ServerCertificate = ' + ISNULL('''' + REPLACE(@ServerCertificate,'''','''''') + '''','NULL')
  SET @Parameters += ', @ServerAsymmetricKey = ' + ISNULL('''' + REPLACE(@ServerAsymmetricKey,'''','''''') + '''','NULL')
  SET @Parameters += ', @EncryptionKey = ' + ISNULL('''' + REPLACE(@EncryptionKey,'''','''''') + '''','NULL')
  SET @Parameters += ', @ReadWriteFileGroups = ' + ISNULL('''' + REPLACE(@ReadWriteFileGroups,'''','''''') + '''','NULL')
  SET @Parameters += ', @OverrideBackupPreference = ' + ISNULL('''' + REPLACE(@OverrideBackupPreference,'''','''''') + '''','NULL')
  SET @Parameters += ', @NoRecovery = ' + ISNULL('''' + REPLACE(@NoRecovery,'''','''''') + '''','NULL')
  SET @Parameters += ', @URL = ' + ISNULL('''' + REPLACE(@URL,'''','''''') + '''','NULL')
  SET @Parameters += ', @Credential = ' + ISNULL('''' + REPLACE(@Credential,'''','''''') + '''','NULL')
  SET @Parameters += ', @MirrorDirectory = ' + ISNULL('''' + REPLACE(@MirrorDirectory,'''','''''') + '''','NULL')
  SET @Parameters += ', @MirrorCleanupTime = ' + ISNULL(CAST(@MirrorCleanupTime AS nvarchar),'NULL')
  SET @Parameters += ', @MirrorCleanupMode = ' + ISNULL('''' + REPLACE(@MirrorCleanupMode,'''','''''') + '''','NULL')
  SET @Parameters += ', @MirrorURL = ' + ISNULL('''' + REPLACE(@MirrorURL,'''','''''') + '''','NULL')
  SET @Parameters += ', @AvailabilityGroups = ' + ISNULL('''' + REPLACE(@AvailabilityGroups,'''','''''') + '''','NULL')
  SET @Parameters += ', @Updateability = ' + ISNULL('''' + REPLACE(@Updateability,'''','''''') + '''','NULL')
  SET @Parameters += ', @AdaptiveCompression = ' + ISNULL('''' + REPLACE(@AdaptiveCompression,'''','''''') + '''','NULL')
  SET @Parameters += ', @ModificationLevel = ' + ISNULL(CAST(@ModificationLevel AS nvarchar),'NULL')
  SET @Parameters += ', @LogSizeSinceLastLogBackup = ' + ISNULL(CAST(@LogSizeSinceLastLogBackup AS nvarchar),'NULL')
  SET @Parameters += ', @TimeSinceLastLogBackup = ' + ISNULL(CAST(@TimeSinceLastLogBackup AS nvarchar),'NULL')
  SET @Parameters += ', @DataDomainBoostHost = ' + ISNULL('''' + REPLACE(@DataDomainBoostHost,'''','''''') + '''','NULL')
  SET @Parameters += ', @DataDomainBoostUser = ' + ISNULL('''' + REPLACE(@DataDomainBoostUser,'''','''''') + '''','NULL')
  SET @Parameters += ', @DataDomainBoostDevicePath = ' + ISNULL('''' + REPLACE(@DataDomainBoostDevicePath,'''','''''') + '''','NULL')
  SET @Parameters += ', @DataDomainBoostLockboxPath = ' + ISNULL('''' + REPLACE(@DataDomainBoostLockboxPath,'''','''''') + '''','NULL')
  SET @Parameters += ', @DirectoryStructure = ' + ISNULL('''' + REPLACE(@DirectoryStructure,'''','''''') + '''','NULL')
  SET @Parameters += ', @AvailabilityGroupDirectoryStructure = ' + ISNULL('''' + REPLACE(@AvailabilityGroupDirectoryStructure,'''','''''') + '''','NULL')
  SET @Parameters += ', @FileName = ' + ISNULL('''' + REPLACE(@FileName,'''','''''') + '''','NULL')
  SET @Parameters += ', @AvailabilityGroupFileName = ' + ISNULL('''' + REPLACE(@AvailabilityGroupFileName,'''','''''') + '''','NULL')
  SET @Parameters += ', @FileExtensionFull = ' + ISNULL('''' + REPLACE(@FileExtensionFull,'''','''''') + '''','NULL')
  SET @Parameters += ', @FileExtensionDiff = ' + ISNULL('''' + REPLACE(@FileExtensionDiff,'''','''''') + '''','NULL')
  SET @Parameters += ', @FileExtensionLog = ' + ISNULL('''' + REPLACE(@FileExtensionLog,'''','''''') + '''','NULL')
  SET @Parameters += ', @Init = ' + ISNULL('''' + REPLACE(@Init,'''','''''') + '''','NULL')
  SET @Parameters += ', @Format = ' + ISNULL('''' + REPLACE(@Format,'''','''''') + '''','NULL')
  SET @Parameters += ', @ObjectLevelRecoveryMap = ' + ISNULL('''' + REPLACE(@ObjectLevelRecoveryMap,'''','''''') + '''','NULL')
  SET @Parameters += ', @ExcludeLogShippedFromLogBackup = ' + ISNULL('''' + REPLACE(@ExcludeLogShippedFromLogBackup,'''','''''') + '''','NULL')
  SET @Parameters += ', @DirectoryCheck = ' + ISNULL('''' + REPLACE(@DirectoryCheck,'''','''''') + '''','NULL')
  SET @Parameters += ', @StringDelimiter = ' + ISNULL('''' + REPLACE(@StringDelimiter,'''','''''') + '''','NULL')
  SET @Parameters += ', @DatabaseOrder = ' + ISNULL('''' + REPLACE(@DatabaseOrder,'''','''''') + '''','NULL')
  SET @Parameters += ', @DatabasesInParallel = ' + ISNULL('''' + REPLACE(@DatabasesInParallel,'''','''''') + '''','NULL')
  SET @Parameters += ', @LogToTable = ' + ISNULL('''' + REPLACE(@LogToTable,'''','''''') + '''','NULL')
  SET @Parameters += ', @Execute = ' + ISNULL('''' + REPLACE(@Execute,'''','''''') + '''','NULL')

  SET @StartMessage = 'Date and time: ' + CONVERT(nvarchar,@StartTime,120)
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Server: ' + CAST(SERVERPROPERTY('ServerName') AS nvarchar(max))
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Version: ' + CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max))
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Edition: ' + CAST(SERVERPROPERTY('Edition') AS nvarchar(max))
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Platform: ' + @HostPlatform
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Procedure: ' + QUOTENAME(DB_NAME(DB_ID())) + '.' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@ObjectName)
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Parameters: ' + @Parameters
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Version: ' + @VersionTimestamp
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Source: Ascent Technology'
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  RAISERROR(@EmptyLine,10,1) WITH NOWAIT

  ----------------------------------------------------------------------------------------------------
  --// Check core requirements                                                                    //--
  ----------------------------------------------------------------------------------------------------

  IF NOT (SELECT [compatibility_level] FROM sys.databases WHERE database_id = DB_ID()) >= 90
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The database ' + QUOTENAME(DB_NAME(DB_ID())) + ' has to be in compatibility level 90 or higher.', 16, 1
  END

  IF NOT (SELECT uses_ansi_nulls FROM sys.sql_modules WHERE [object_id] = @@PROCID) = 1
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'ANSI_NULLS has to be set to ON for the stored procedure.', 16, 1
  END

  IF NOT (SELECT uses_quoted_identifier FROM sys.sql_modules WHERE [object_id] = @@PROCID) = 1
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'QUOTED_IDENTIFIER has to be set to ON for the stored procedure.', 16, 1
  END

  IF NOT EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'P' AND schemas.[name] = 'dbo' AND objects.[name] = 'CommandExecute')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The stored procedure CommandExecute is missing. Contact your Ascent Technology Consultant. https://www.ascent.tech ', 16, 1
  END

  IF EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'P' AND schemas.[name] = 'dbo' AND objects.[name] = 'CommandExecute' AND OBJECT_DEFINITION(objects.[object_id]) NOT LIKE '%@DatabaseContext%')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The stored procedure CommandExecute needs to be updated. Contact your Ascent Technology Consultant. https://www.ascent.tech ', 16, 1
  END

  IF @LogToTable = 'Y' AND NOT EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'U' AND schemas.[name] = 'dbo' AND objects.[name] = 'CommandLog')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The table CommandLog is missing. Contact your Ascent Technology Consultant. https://www.ascent.tech ', 16, 1
  END

  IF @DatabasesInParallel = 'Y' AND NOT EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'U' AND schemas.[name] = 'dbo' AND objects.[name] = 'Queue')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The table Queue is missing. Contact your Ascent Technology Consultant. https://www.ascent.tech ', 16, 1
  END

  IF @DatabasesInParallel = 'Y' AND NOT EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'U' AND schemas.[name] = 'dbo' AND objects.[name] = 'QueueDatabase')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The table QueueDatabase is missing. Contact your Ascent Technology Consultant. https://www.ascent.tech ', 16, 1
  END

  IF @@TRANCOUNT <> 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The transaction count is not 0.', 16, 1
  END

  IF @AmazonRDS = 1
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The stored procedure DatabaseBackup is not supported on Amazon RDS.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------
  --// Select databases                                                                           //--
  ----------------------------------------------------------------------------------------------------

  SET @Databases = REPLACE(@Databases, CHAR(10), '')
  SET @Databases = REPLACE(@Databases, CHAR(13), '')

  WHILE CHARINDEX(@StringDelimiter + ' ', @Databases) > 0 SET @Databases = REPLACE(@Databases, @StringDelimiter + ' ', @StringDelimiter)
  WHILE CHARINDEX(' ' + @StringDelimiter, @Databases) > 0 SET @Databases = REPLACE(@Databases, ' ' + @StringDelimiter, @StringDelimiter)

  SET @Databases = LTRIM(RTRIM(@Databases));

  WITH Databases1 (StartPosition, EndPosition, DatabaseItem) AS
  (
  SELECT 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Databases, 1), 0), LEN(@Databases) + 1) AS EndPosition,
         SUBSTRING(@Databases, 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Databases, 1), 0), LEN(@Databases) + 1) - 1) AS DatabaseItem
  WHERE @Databases IS NOT NULL
  UNION ALL
  SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Databases, EndPosition + 1), 0), LEN(@Databases) + 1) AS EndPosition,
         SUBSTRING(@Databases, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Databases, EndPosition + 1), 0), LEN(@Databases) + 1) - EndPosition - 1) AS DatabaseItem
  FROM Databases1
  WHERE EndPosition < LEN(@Databases) + 1
  ),
  Databases2 (DatabaseItem, StartPosition, Selected) AS
  (
  SELECT CASE WHEN DatabaseItem LIKE '-%' THEN RIGHT(DatabaseItem,LEN(DatabaseItem) - 1) ELSE DatabaseItem END AS DatabaseItem,
         StartPosition,
         CASE WHEN DatabaseItem LIKE '-%' THEN 0 ELSE 1 END AS Selected
  FROM Databases1
  ),
  Databases3 (DatabaseItem, DatabaseType, AvailabilityGroup, StartPosition, Selected) AS
  (
  SELECT CASE WHEN DatabaseItem IN('ALL_DATABASES','SYSTEM_DATABASES','USER_DATABASES','AVAILABILITY_GROUP_DATABASES') THEN '%' ELSE DatabaseItem END AS DatabaseItem,
         CASE WHEN DatabaseItem = 'SYSTEM_DATABASES' THEN 'S' WHEN DatabaseItem = 'USER_DATABASES' THEN 'U' ELSE NULL END AS DatabaseType,
         CASE WHEN DatabaseItem = 'AVAILABILITY_GROUP_DATABASES' THEN 1 ELSE NULL END AvailabilityGroup,
         StartPosition,
         Selected
  FROM Databases2
  ),
  Databases4 (DatabaseName, DatabaseType, AvailabilityGroup, StartPosition, Selected) AS
  (
  SELECT CASE WHEN LEFT(DatabaseItem,1) = '[' AND RIGHT(DatabaseItem,1) = ']' THEN PARSENAME(DatabaseItem,1) ELSE DatabaseItem END AS DatabaseItem,
         DatabaseType,
         AvailabilityGroup,
         StartPosition,
         Selected
  FROM Databases3
  )
  INSERT INTO @SelectedDatabases (DatabaseName, DatabaseType, AvailabilityGroup, StartPosition, Selected)
  SELECT DatabaseName,
         DatabaseType,
         AvailabilityGroup,
         StartPosition,
         Selected
  FROM Databases4
  OPTION (MAXRECURSION 0)

  IF @Version >= 11 AND SERVERPROPERTY('IsHadrEnabled') = 1
  BEGIN
    INSERT INTO @tmpAvailabilityGroups (AvailabilityGroupName, Selected)
    SELECT name AS AvailabilityGroupName,
           0 AS Selected
    FROM sys.availability_groups

    INSERT INTO @tmpDatabasesAvailabilityGroups (DatabaseName, AvailabilityGroupName)
    SELECT databases.name,
           availability_groups.name
    FROM sys.databases databases
    INNER JOIN sys.availability_replicas availability_replicas ON databases.replica_id = availability_replicas.replica_id
    INNER JOIN sys.availability_groups availability_groups ON availability_replicas.group_id = availability_groups.group_id
  END

  INSERT INTO @tmpDatabases (DatabaseName, DatabaseNameFS, DatabaseType, AvailabilityGroup, [Order], Selected, Completed)
  SELECT [name] AS DatabaseName,
         RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE([name],'\',''),'/',''),':',''),'*',''),'?',''),'"',''),'<',''),'>',''),'|','')) AS DatabaseNameFS,
         CASE WHEN name IN('master','msdb','model') OR is_distributor = 1 THEN 'S' ELSE 'U' END AS DatabaseType,
         NULL AS AvailabilityGroup,
         0 AS [Order],
         0 AS Selected,
         0 AS Completed
  FROM sys.databases
  WHERE [name] <> 'tempdb'
  AND source_database_id IS NULL
  ORDER BY [name] ASC

  UPDATE tmpDatabases
  SET AvailabilityGroup = CASE WHEN EXISTS (SELECT * FROM @tmpDatabasesAvailabilityGroups WHERE DatabaseName = tmpDatabases.DatabaseName) THEN 1 ELSE 0 END
  FROM @tmpDatabases tmpDatabases

  UPDATE tmpDatabases
  SET tmpDatabases.Selected = SelectedDatabases.Selected
  FROM @tmpDatabases tmpDatabases
  INNER JOIN @SelectedDatabases SelectedDatabases
  ON tmpDatabases.DatabaseName LIKE REPLACE(SelectedDatabases.DatabaseName,'_','[_]')
  AND (tmpDatabases.DatabaseType = SelectedDatabases.DatabaseType OR SelectedDatabases.DatabaseType IS NULL)
  AND (tmpDatabases.AvailabilityGroup = SelectedDatabases.AvailabilityGroup OR SelectedDatabases.AvailabilityGroup IS NULL)
  WHERE SelectedDatabases.Selected = 1

  UPDATE tmpDatabases
  SET tmpDatabases.Selected = SelectedDatabases.Selected
  FROM @tmpDatabases tmpDatabases
  INNER JOIN @SelectedDatabases SelectedDatabases
  ON tmpDatabases.DatabaseName LIKE REPLACE(SelectedDatabases.DatabaseName,'_','[_]')
  AND (tmpDatabases.DatabaseType = SelectedDatabases.DatabaseType OR SelectedDatabases.DatabaseType IS NULL)
  AND (tmpDatabases.AvailabilityGroup = SelectedDatabases.AvailabilityGroup OR SelectedDatabases.AvailabilityGroup IS NULL)
  WHERE SelectedDatabases.Selected = 0

  UPDATE tmpDatabases
  SET tmpDatabases.StartPosition = SelectedDatabases2.StartPosition
  FROM @tmpDatabases tmpDatabases
  INNER JOIN (SELECT tmpDatabases.DatabaseName, MIN(SelectedDatabases.StartPosition) AS StartPosition
              FROM @tmpDatabases tmpDatabases
              INNER JOIN @SelectedDatabases SelectedDatabases
              ON tmpDatabases.DatabaseName LIKE REPLACE(SelectedDatabases.DatabaseName,'_','[_]')
              AND (tmpDatabases.DatabaseType = SelectedDatabases.DatabaseType OR SelectedDatabases.DatabaseType IS NULL)
              AND (tmpDatabases.AvailabilityGroup = SelectedDatabases.AvailabilityGroup OR SelectedDatabases.AvailabilityGroup IS NULL)
              WHERE SelectedDatabases.Selected = 1
              GROUP BY tmpDatabases.DatabaseName) SelectedDatabases2
  ON tmpDatabases.DatabaseName = SelectedDatabases2.DatabaseName

  IF @Databases IS NOT NULL AND (NOT EXISTS(SELECT * FROM @SelectedDatabases) OR EXISTS(SELECT * FROM @SelectedDatabases WHERE DatabaseName IS NULL OR DatabaseName = ''))
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Databases is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------
  --// Select availability groups                                                                 //--
  ----------------------------------------------------------------------------------------------------

  IF @AvailabilityGroups IS NOT NULL AND @Version >= 11 AND SERVERPROPERTY('IsHadrEnabled') = 1
  BEGIN

    SET @AvailabilityGroups = REPLACE(@AvailabilityGroups, CHAR(10), '')
    SET @AvailabilityGroups = REPLACE(@AvailabilityGroups, CHAR(13), '')

    WHILE CHARINDEX(@StringDelimiter + ' ', @AvailabilityGroups) > 0 SET @AvailabilityGroups = REPLACE(@AvailabilityGroups, @StringDelimiter + ' ', @StringDelimiter)
    WHILE CHARINDEX(' ' + @StringDelimiter, @AvailabilityGroups) > 0 SET @AvailabilityGroups = REPLACE(@AvailabilityGroups, ' ' + @StringDelimiter, @StringDelimiter)

    SET @AvailabilityGroups = LTRIM(RTRIM(@AvailabilityGroups));

    WITH AvailabilityGroups1 (StartPosition, EndPosition, AvailabilityGroupItem) AS
    (
    SELECT 1 AS StartPosition,
           ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @AvailabilityGroups, 1), 0), LEN(@AvailabilityGroups) + 1) AS EndPosition,
           SUBSTRING(@AvailabilityGroups, 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @AvailabilityGroups, 1), 0), LEN(@AvailabilityGroups) + 1) - 1) AS AvailabilityGroupItem
    WHERE @AvailabilityGroups IS NOT NULL
    UNION ALL
    SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
           ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @AvailabilityGroups, EndPosition + 1), 0), LEN(@AvailabilityGroups) + 1) AS EndPosition,
           SUBSTRING(@AvailabilityGroups, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @AvailabilityGroups, EndPosition + 1), 0), LEN(@AvailabilityGroups) + 1) - EndPosition - 1) AS AvailabilityGroupItem
    FROM AvailabilityGroups1
    WHERE EndPosition < LEN(@AvailabilityGroups) + 1
    ),
    AvailabilityGroups2 (AvailabilityGroupItem, StartPosition, Selected) AS
    (
    SELECT CASE WHEN AvailabilityGroupItem LIKE '-%' THEN RIGHT(AvailabilityGroupItem,LEN(AvailabilityGroupItem) - 1) ELSE AvailabilityGroupItem END AS AvailabilityGroupItem,
           StartPosition,
           CASE WHEN AvailabilityGroupItem LIKE '-%' THEN 0 ELSE 1 END AS Selected
    FROM AvailabilityGroups1
    ),
    AvailabilityGroups3 (AvailabilityGroupItem, StartPosition, Selected) AS
    (
    SELECT CASE WHEN AvailabilityGroupItem = 'ALL_AVAILABILITY_GROUPS' THEN '%' ELSE AvailabilityGroupItem END AS AvailabilityGroupItem,
           StartPosition,
           Selected
    FROM AvailabilityGroups2
    ),
    AvailabilityGroups4 (AvailabilityGroupName, StartPosition, Selected) AS
    (
    SELECT CASE WHEN LEFT(AvailabilityGroupItem,1) = '[' AND RIGHT(AvailabilityGroupItem,1) = ']' THEN PARSENAME(AvailabilityGroupItem,1) ELSE AvailabilityGroupItem END AS AvailabilityGroupItem,
           StartPosition,
           Selected
    FROM AvailabilityGroups3
    )
    INSERT INTO @SelectedAvailabilityGroups (AvailabilityGroupName, StartPosition, Selected)
    SELECT AvailabilityGroupName, StartPosition, Selected
    FROM AvailabilityGroups4
    OPTION (MAXRECURSION 0)

    UPDATE tmpAvailabilityGroups
    SET tmpAvailabilityGroups.Selected = SelectedAvailabilityGroups.Selected
    FROM @tmpAvailabilityGroups tmpAvailabilityGroups
    INNER JOIN @SelectedAvailabilityGroups SelectedAvailabilityGroups
    ON tmpAvailabilityGroups.AvailabilityGroupName LIKE REPLACE(SelectedAvailabilityGroups.AvailabilityGroupName,'_','[_]')
    WHERE SelectedAvailabilityGroups.Selected = 1

    UPDATE tmpAvailabilityGroups
    SET tmpAvailabilityGroups.Selected = SelectedAvailabilityGroups.Selected
    FROM @tmpAvailabilityGroups tmpAvailabilityGroups
    INNER JOIN @SelectedAvailabilityGroups SelectedAvailabilityGroups
    ON tmpAvailabilityGroups.AvailabilityGroupName LIKE REPLACE(SelectedAvailabilityGroups.AvailabilityGroupName,'_','[_]')
    WHERE SelectedAvailabilityGroups.Selected = 0

    UPDATE tmpAvailabilityGroups
    SET tmpAvailabilityGroups.StartPosition = SelectedAvailabilityGroups2.StartPosition
    FROM @tmpAvailabilityGroups tmpAvailabilityGroups
    INNER JOIN (SELECT tmpAvailabilityGroups.AvailabilityGroupName, MIN(SelectedAvailabilityGroups.StartPosition) AS StartPosition
                FROM @tmpAvailabilityGroups tmpAvailabilityGroups
                INNER JOIN @SelectedAvailabilityGroups SelectedAvailabilityGroups
                ON tmpAvailabilityGroups.AvailabilityGroupName LIKE REPLACE(SelectedAvailabilityGroups.AvailabilityGroupName,'_','[_]')
                WHERE SelectedAvailabilityGroups.Selected = 1
                GROUP BY tmpAvailabilityGroups.AvailabilityGroupName) SelectedAvailabilityGroups2
    ON tmpAvailabilityGroups.AvailabilityGroupName = SelectedAvailabilityGroups2.AvailabilityGroupName

    UPDATE tmpDatabases
    SET tmpDatabases.StartPosition = tmpAvailabilityGroups.StartPosition,
        tmpDatabases.Selected = 1
    FROM @tmpDatabases tmpDatabases
    INNER JOIN @tmpDatabasesAvailabilityGroups tmpDatabasesAvailabilityGroups ON tmpDatabases.DatabaseName = tmpDatabasesAvailabilityGroups.DatabaseName
    INNER JOIN @tmpAvailabilityGroups tmpAvailabilityGroups ON tmpDatabasesAvailabilityGroups.AvailabilityGroupName = tmpAvailabilityGroups.AvailabilityGroupName
    WHERE tmpAvailabilityGroups.Selected = 1

  END

  IF @AvailabilityGroups IS NOT NULL AND (NOT EXISTS(SELECT * FROM @SelectedAvailabilityGroups) OR EXISTS(SELECT * FROM @SelectedAvailabilityGroups WHERE AvailabilityGroupName IS NULL OR AvailabilityGroupName = '') OR @Version < 11 OR SERVERPROPERTY('IsHadrEnabled') = 0)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @AvailabilityGroups is not supported.', 16, 1
  END

  IF (@Databases IS NULL AND @AvailabilityGroups IS NULL)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'You need to specify one of the parameters @Databases and @AvailabilityGroups.', 16, 2
  END

  IF (@Databases IS NOT NULL AND @AvailabilityGroups IS NOT NULL)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'You can only specify one of the parameters @Databases and @AvailabilityGroups.', 16, 3
  END

  ----------------------------------------------------------------------------------------------------
  --// Check database names                                                                       //--
  ----------------------------------------------------------------------------------------------------

  SET @ErrorMessage = ''
  SELECT @ErrorMessage = @ErrorMessage + QUOTENAME(DatabaseName) + ', '
  FROM @tmpDatabases
  WHERE Selected = 1
  AND DatabaseNameFS = ''
  ORDER BY DatabaseName ASC
  IF @@ROWCOUNT > 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The names of the following databases are not supported: ' + LEFT(@ErrorMessage,LEN(@ErrorMessage)-1) + '.', 16, 1
  END

  SET @ErrorMessage = ''
  SELECT @ErrorMessage = @ErrorMessage + QUOTENAME(DatabaseName) + ', '
  FROM @tmpDatabases
  WHERE UPPER(DatabaseNameFS) IN(SELECT UPPER(DatabaseNameFS) FROM @tmpDatabases GROUP BY UPPER(DatabaseNameFS) HAVING COUNT(*) > 1)
  AND UPPER(DatabaseNameFS) IN(SELECT UPPER(DatabaseNameFS) FROM @tmpDatabases WHERE Selected = 1)
  AND DatabaseNameFS <> ''
  ORDER BY DatabaseName ASC
  OPTION (RECOMPILE)
  IF @@ROWCOUNT > 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The names of the following databases are not unique in the file system: ' + LEFT(@ErrorMessage,LEN(@ErrorMessage)-1) + '.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------
  --// Select default directory                                                                      //--
  ----------------------------------------------------------------------------------------------------

  IF @Directory IS NULL AND @URL IS NULL AND (@BackupSoftware <> 'DATA_DOMAIN_BOOST' OR @BackupSoftware IS NULL)
  BEGIN
    IF @Version >= 15
    BEGIN
      SET @DefaultDirectory = CAST(SERVERPROPERTY('InstanceDefaultBackupPath') AS nvarchar(max))
    END
    ELSE
    BEGIN
      EXECUTE [master].dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE', N'SOFTWARE\Microsoft\MSSQLServer\MSSQLServer', N'BackupDirectory', @DefaultDirectory OUTPUT
    END

    IF @DefaultDirectory LIKE 'http://%' OR @DefaultDirectory LIKE 'https://%'
    BEGIN
      SET @URL = @DefaultDirectory
    END
    ELSE
    BEGIN
      INSERT INTO @Directories (ID, DirectoryPath, Mirror, Completed)
      SELECT 1, @DefaultDirectory, 0, 0
    END
  END

  ----------------------------------------------------------------------------------------------------
  --// Select directories                                                                         //--
  ----------------------------------------------------------------------------------------------------

  SET @Directory = REPLACE(@Directory, CHAR(10), '')
  SET @Directory = REPLACE(@Directory, CHAR(13), '')

  WHILE CHARINDEX(@StringDelimiter + ' ', @Directory) > 0 SET @Directory = REPLACE(@Directory, @StringDelimiter + ' ', @StringDelimiter)
  WHILE CHARINDEX(' ' + @StringDelimiter, @Directory) > 0 SET @Directory = REPLACE(@Directory, ' ' + @StringDelimiter, @StringDelimiter)

  SET @Directory = LTRIM(RTRIM(@Directory));

  WITH Directories (StartPosition, EndPosition, Directory) AS
  (
  SELECT 1 AS StartPosition,
          ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Directory, 1), 0), LEN(@Directory) + 1) AS EndPosition,
          SUBSTRING(@Directory, 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Directory, 1), 0), LEN(@Directory) + 1) - 1) AS Directory
  WHERE @Directory IS NOT NULL
  UNION ALL
  SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
          ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Directory, EndPosition + 1), 0), LEN(@Directory) + 1) AS EndPosition,
          SUBSTRING(@Directory, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Directory, EndPosition + 1), 0), LEN(@Directory) + 1) - EndPosition - 1) AS Directory
  FROM Directories
  WHERE EndPosition < LEN(@Directory) + 1
  )
  INSERT INTO @Directories (ID, DirectoryPath, Mirror, Completed)
  SELECT ROW_NUMBER() OVER(ORDER BY StartPosition ASC) AS ID,
          Directory,
          0,
          0
  FROM Directories
  OPTION (MAXRECURSION 0)

  SET @MirrorDirectory = REPLACE(@MirrorDirectory, CHAR(10), '')
  SET @MirrorDirectory = REPLACE(@MirrorDirectory, CHAR(13), '')

  WHILE CHARINDEX(', ',@MirrorDirectory) > 0 SET @MirrorDirectory = REPLACE(@MirrorDirectory,', ',',')
  WHILE CHARINDEX(' ,',@MirrorDirectory) > 0 SET @MirrorDirectory = REPLACE(@MirrorDirectory,' ,',',')

  SET @MirrorDirectory = LTRIM(RTRIM(@MirrorDirectory));

  WITH Directories (StartPosition, EndPosition, Directory) AS
  (
  SELECT 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @MirrorDirectory, 1), 0), LEN(@MirrorDirectory) + 1) AS EndPosition,
         SUBSTRING(@MirrorDirectory, 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @MirrorDirectory, 1), 0), LEN(@MirrorDirectory) + 1) - 1) AS Directory
  WHERE @MirrorDirectory IS NOT NULL
  UNION ALL
  SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @MirrorDirectory, EndPosition + 1), 0), LEN(@MirrorDirectory) + 1) AS EndPosition,
         SUBSTRING(@MirrorDirectory, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @MirrorDirectory, EndPosition + 1), 0), LEN(@MirrorDirectory) + 1) - EndPosition - 1) AS Directory
  FROM Directories
  WHERE EndPosition < LEN(@MirrorDirectory) + 1
  )
  INSERT INTO @Directories (ID, DirectoryPath, Mirror, Completed)
  SELECT (SELECT COUNT(*) FROM @Directories) + ROW_NUMBER() OVER(ORDER BY StartPosition ASC) AS ID,
         Directory,
         1,
         0
  FROM Directories
  OPTION (MAXRECURSION 0)

  ----------------------------------------------------------------------------------------------------
  --// Check directories                                                                          //--
  ----------------------------------------------------------------------------------------------------

  IF EXISTS (SELECT * FROM @Directories WHERE Mirror = 0 AND (NOT (DirectoryPath LIKE '_:' OR DirectoryPath LIKE '_:\%' OR DirectoryPath LIKE '\\%\%' OR (DirectoryPath LIKE '/%/%' AND @HostPlatform = 'Linux') OR DirectoryPath = 'NUL') OR DirectoryPath IS NULL OR LEFT(DirectoryPath,1) = ' ' OR RIGHT(DirectoryPath,1) = ' '))
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Directory is not supported.', 16, 1
  END

  IF EXISTS (SELECT * FROM @Directories GROUP BY DirectoryPath HAVING COUNT(*) <> 1)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Directory is not supported.', 16, 2
  END

  IF (SELECT COUNT(*) FROM @Directories WHERE Mirror = 0) <> (SELECT COUNT(*) FROM @Directories WHERE Mirror = 1) AND (SELECT COUNT(*) FROM @Directories WHERE Mirror = 1) > 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The number of directories for the parameters @Directory and @MirrorDirectory has to be the same.', 16, 3
  END

  IF (@Directory IS NOT NULL AND SERVERPROPERTY('EngineEdition') = 8) OR (@Directory IS NOT NULL AND @BackupSoftware = 'DATA_DOMAIN_BOOST')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Directory is not supported.', 16, 4
  END

  IF EXISTS (SELECT * FROM @Directories WHERE Mirror = 0 AND DirectoryPath = 'NUL') AND EXISTS(SELECT * FROM @Directories WHERE Mirror = 0 AND DirectoryPath <> 'NUL')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Directory is not supported.', 16, 5
  END

  IF EXISTS (SELECT * FROM @Directories WHERE Mirror = 0 AND DirectoryPath = 'NUL') AND EXISTS(SELECT * FROM @Directories WHERE Mirror = 1)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'Mirrored backup is not supported when backing up to NUL', 16, 6
  END

  ----------------------------------------------------------------------------------------------------

  IF EXISTS(SELECT * FROM @Directories WHERE Mirror = 1 AND (NOT (DirectoryPath LIKE '_:' OR DirectoryPath LIKE '_:\%' OR DirectoryPath LIKE '\\%\%' OR (DirectoryPath LIKE '/%/%' AND @HostPlatform = 'Linux')) OR DirectoryPath IS NULL OR LEFT(DirectoryPath,1) = ' ' OR RIGHT(DirectoryPath,1) = ' '))
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @MirrorDirectory is not supported.', 16, 1
  END

  IF EXISTS (SELECT * FROM @Directories GROUP BY DirectoryPath HAVING COUNT(*) <> 1)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @MirrorDirectory is not supported.', 16, 2
  END

  IF @BackupSoftware IN('SQLBACKUP','SQLSAFE') AND (SELECT COUNT(*) FROM @Directories WHERE Mirror = 1) > 1
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @MirrorDirectory is not supported.', 16, 4
  END

  IF @MirrorDirectory IS NOT NULL AND SERVERPROPERTY('EngineEdition') = 8
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @MirrorDirectory is not supported.', 16, 5
  END

  IF @MirrorDirectory IS NOT NULL AND @BackupSoftware = 'DATA_DOMAIN_BOOST'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @MirrorDirectory is not supported.', 16, 6
  END

  IF (@BackupSoftware IS NULL AND EXISTS(SELECT * FROM @Directories WHERE Mirror = 1) AND SERVERPROPERTY('EngineEdition') <> 3)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @MirrorDirectory is not supported. Mirrored backup to disk is only available in Enterprise and Developer Edition.', 16, 8
  END

  ----------------------------------------------------------------------------------------------------

  IF NOT EXISTS (SELECT * FROM @Errors WHERE Severity >= 16) AND @DirectoryCheck = 'Y'
  BEGIN
    WHILE (1 = 1)
    BEGIN
      SELECT TOP 1 @CurrentRootDirectoryID = ID,
                   @CurrentRootDirectoryPath = DirectoryPath
      FROM @Directories
      WHERE Completed = 0
      AND DirectoryPath <> 'NUL'
      ORDER BY ID ASC

      IF @@ROWCOUNT = 0
      BEGIN
        BREAK
      END

      INSERT INTO @DirectoryInfo (FileExists, FileIsADirectory, ParentDirectoryExists)
      EXECUTE [master].dbo.xp_fileexist @CurrentRootDirectoryPath

      IF NOT EXISTS (SELECT * FROM @DirectoryInfo WHERE FileExists = 0 AND FileIsADirectory = 1 AND ParentDirectoryExists = 1)
      BEGIN
        INSERT INTO @Errors ([Message], Severity, [State])
        SELECT 'The directory ' + @CurrentRootDirectoryPath + ' does not exist.', 16, 1
      END

      UPDATE @Directories
      SET Completed = 1
      WHERE ID = @CurrentRootDirectoryID

      SET @CurrentRootDirectoryID = NULL
      SET @CurrentRootDirectoryPath = NULL

      DELETE FROM @DirectoryInfo
    END
  END

  ----------------------------------------------------------------------------------------------------
  --// Select URLs                                                                                //--
  ----------------------------------------------------------------------------------------------------

  SET @URL = REPLACE(@URL, CHAR(10), '')
  SET @URL = REPLACE(@URL, CHAR(13), '')

  WHILE CHARINDEX(@StringDelimiter + ' ', @URL) > 0 SET @URL = REPLACE(@URL, @StringDelimiter + ' ', @StringDelimiter)
  WHILE CHARINDEX(' ' + @StringDelimiter, @URL) > 0 SET @URL = REPLACE(@URL, ' ' + @StringDelimiter, @StringDelimiter)

  SET @URL = LTRIM(RTRIM(@URL));

  WITH URLs (StartPosition, EndPosition, [URL]) AS
  (
  SELECT 1 AS StartPosition,
          ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @URL, 1), 0), LEN(@URL) + 1) AS EndPosition,
          SUBSTRING(@URL, 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @URL, 1), 0), LEN(@URL) + 1) - 1) AS [URL]
  WHERE @URL IS NOT NULL
  UNION ALL
  SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
          ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @URL, EndPosition + 1), 0), LEN(@URL) + 1) AS EndPosition,
          SUBSTRING(@URL, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @URL, EndPosition + 1), 0), LEN(@URL) + 1) - EndPosition - 1) AS [URL]
  FROM URLs
  WHERE EndPosition < LEN(@URL) + 1
  )
  INSERT INTO @URLs (ID, DirectoryPath, Mirror)
  SELECT ROW_NUMBER() OVER(ORDER BY StartPosition ASC) AS ID,
          [URL],
          0
  FROM URLs
  OPTION (MAXRECURSION 0)

  SET @MirrorURL = REPLACE(@MirrorURL, CHAR(10), '')
  SET @MirrorURL = REPLACE(@MirrorURL, CHAR(13), '')

  WHILE CHARINDEX(@StringDelimiter + ' ', @MirrorURL) > 0 SET @MirrorURL = REPLACE(@MirrorURL, @StringDelimiter + ' ', @StringDelimiter)
  WHILE CHARINDEX(' ' + @StringDelimiter ,@MirrorURL) > 0 SET @MirrorURL = REPLACE(@MirrorURL, ' ' + @StringDelimiter, @StringDelimiter)

  SET @MirrorURL = LTRIM(RTRIM(@MirrorURL));

  WITH URLs (StartPosition, EndPosition, [URL]) AS
  (
  SELECT 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @MirrorURL, 1), 0), LEN(@MirrorURL) + 1) AS EndPosition,
         SUBSTRING(@MirrorURL, 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @MirrorURL, 1), 0), LEN(@MirrorURL) + 1) - 1) AS [URL]
  WHERE @MirrorURL IS NOT NULL
  UNION ALL
  SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @MirrorURL, EndPosition + 1), 0), LEN(@MirrorURL) + 1) AS EndPosition,
         SUBSTRING(@MirrorURL, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @MirrorURL, EndPosition + 1), 0), LEN(@MirrorURL) + 1) - EndPosition - 1) AS [URL]
  FROM URLs
  WHERE EndPosition < LEN(@MirrorURL) + 1
  )
  INSERT INTO @URLs (ID, DirectoryPath, Mirror)
  SELECT (SELECT COUNT(*) FROM @URLs) + ROW_NUMBER() OVER(ORDER BY StartPosition ASC) AS ID,
         [URL],
         1
  FROM URLs
  OPTION (MAXRECURSION 0)

  ----------------------------------------------------------------------------------------------------
  --// Check URLs                                                                          //--
  ----------------------------------------------------------------------------------------------------

  IF EXISTS(SELECT * FROM @URLs WHERE Mirror = 0 AND DirectoryPath NOT LIKE 'https://%/%')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @URL is not supported.', 16, 1
  END

  IF EXISTS (SELECT * FROM @URLs GROUP BY DirectoryPath HAVING COUNT(*) <> 1)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @URL is not supported.', 16, 2
  END

  IF (SELECT COUNT(*) FROM @URLs WHERE Mirror = 0) <> (SELECT COUNT(*) FROM @URLs WHERE Mirror = 1) AND (SELECT COUNT(*) FROM @URLs WHERE Mirror = 1) > 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @URL is not supported.', 16, 3
  END

  ----------------------------------------------------------------------------------------------------

  IF EXISTS(SELECT * FROM @URLs WHERE Mirror = 1 AND DirectoryPath NOT LIKE 'https://%/%')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @MirrorURL is not supported.', 16, 1
  END

  IF EXISTS (SELECT * FROM @URLs GROUP BY DirectoryPath HAVING COUNT(*) <> 1)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @MirrorURL is not supported.', 16, 2
  END

  IF (SELECT COUNT(*) FROM @URLs WHERE Mirror = 0) <> (SELECT COUNT(*) FROM @URLs WHERE Mirror = 1) AND (SELECT COUNT(*) FROM @URLs WHERE Mirror = 1) > 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @MirrorURL is not supported.', 16, 3
  END

  ----------------------------------------------------------------------------------------------------
  --// Get directory separator                                                                   //--
  ----------------------------------------------------------------------------------------------------

  SELECT @DirectorySeparator = CASE
  WHEN @URL IS NOT NULL THEN '/'
  WHEN @HostPlatform = 'Windows' THEN '\'
  WHEN @HostPlatform = 'Linux' THEN '/'
  END

  UPDATE @Directories
  SET DirectoryPath = LEFT(DirectoryPath,LEN(DirectoryPath) - 1)
  WHERE RIGHT(DirectoryPath,1) = @DirectorySeparator

  UPDATE @URLs
  SET DirectoryPath = LEFT(DirectoryPath,LEN(DirectoryPath) - 1)
  WHERE RIGHT(DirectoryPath,1) = @DirectorySeparator

  ----------------------------------------------------------------------------------------------------
  --// Get file extension                                                                         //--
  ----------------------------------------------------------------------------------------------------

  IF @FileExtensionFull IS NULL
  BEGIN
    SELECT @FileExtensionFull = CASE
    WHEN @BackupSoftware IS NULL THEN 'bak'
    WHEN @BackupSoftware = 'LITESPEED' THEN 'bak'
    WHEN @BackupSoftware = 'SQLBACKUP' THEN 'sqb'
    WHEN @BackupSoftware = 'SQLSAFE' THEN 'safe'
    END
  END

  IF @FileExtensionDiff IS NULL
  BEGIN
    SELECT @FileExtensionDiff = CASE
    WHEN @BackupSoftware IS NULL THEN 'bak'
    WHEN @BackupSoftware = 'LITESPEED' THEN 'bak'
    WHEN @BackupSoftware = 'SQLBACKUP' THEN 'sqb'
    WHEN @BackupSoftware = 'SQLSAFE' THEN 'safe'
    END
  END

  IF @FileExtensionLog IS NULL
  BEGIN
    SELECT @FileExtensionLog = CASE
    WHEN @BackupSoftware IS NULL THEN 'trn'
    WHEN @BackupSoftware = 'LITESPEED' THEN 'trn'
    WHEN @BackupSoftware = 'SQLBACKUP' THEN 'sqb'
    WHEN @BackupSoftware = 'SQLSAFE' THEN 'safe'
    END
  END

  ----------------------------------------------------------------------------------------------------
  --// Get default compression                                                                    //--
  ----------------------------------------------------------------------------------------------------

  IF @Compress IS NULL
  BEGIN
    SELECT @Compress = CASE WHEN @BackupSoftware IS NULL AND EXISTS(SELECT * FROM sys.configurations WHERE name = 'backup compression default' AND value_in_use = 1) THEN 'Y'
                            WHEN @BackupSoftware IS NULL AND NOT EXISTS(SELECT * FROM sys.configurations WHERE name = 'backup compression default' AND value_in_use = 1) THEN 'N'
                            WHEN @BackupSoftware IS NOT NULL AND (@CompressionLevel IS NULL OR @CompressionLevel > 0)  THEN 'Y'
                            WHEN @BackupSoftware IS NOT NULL AND @CompressionLevel = 0  THEN 'N' END
  END

  ----------------------------------------------------------------------------------------------------
  --// Check input parameters                                                                     //--
  ----------------------------------------------------------------------------------------------------

  IF @BackupType NOT IN ('FULL','DIFF','LOG') OR @BackupType IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @BackupType is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF SERVERPROPERTY('EngineEdition') = 8 AND NOT (@BackupType = 'FULL' AND @CopyOnly = 'Y')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'SQL Database Managed Instance only supports COPY_ONLY full backups.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @Verify NOT IN ('Y','N') OR @Verify IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Verify is not supported.', 16, 1
  END

  IF @BackupSoftware = 'SQLSAFE' AND @Encrypt = 'Y' AND @Verify = 'Y'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Verify is not supported. Verify is not supported with encrypted backups with Idera SQL Safe Backup', 16, 2
  END

  IF @Verify = 'Y' AND @BackupSoftware = 'DATA_DOMAIN_BOOST'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Verify is not supported. Verify is not supported with Data Domain Boost', 16, 3
  END

  IF @Verify = 'Y' AND EXISTS(SELECT * FROM @Directories WHERE DirectoryPath = 'NUL')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Verify is not supported. Verify is not supported when backing up to NUL.', 16, 4
  END

  ----------------------------------------------------------------------------------------------------

  IF @CleanupTime < 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @CleanupTime is not supported.', 16, 1
  END

  IF @CleanupTime IS NOT NULL AND @URL IS NOT NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @CleanupTime is not supported. Cleanup is not supported on Azure Blob Storage.', 16, 2
  END

  IF @CleanupTime IS NOT NULL AND EXISTS(SELECT * FROM @Directories WHERE DirectoryPath = 'NUL')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @CleanupTime is not supported. Cleanup is not supported when backing up to NUL.', 16, 4
  END

  IF @CleanupTime IS NOT NULL AND ((@DirectoryStructure NOT LIKE '%{DatabaseName}%' OR @DirectoryStructure IS NULL) OR (SERVERPROPERTY('IsHadrEnabled') = 1 AND (@AvailabilityGroupDirectoryStructure NOT LIKE '%{DatabaseName}%' OR @AvailabilityGroupDirectoryStructure IS NULL)))
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @CleanupTime is not supported. Cleanup is not supported if the token {DatabaseName} is not part of the directory.', 16, 5
  END

  IF @CleanupTime IS NOT NULL AND ((@DirectoryStructure NOT LIKE '%{BackupType}%' OR @DirectoryStructure IS NULL) OR (SERVERPROPERTY('IsHadrEnabled') = 1 AND (@AvailabilityGroupDirectoryStructure NOT LIKE '%{BackupType}%' OR @AvailabilityGroupDirectoryStructure IS NULL)))
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @CleanupTime is not supported. Cleanup is not supported if the token {BackupType} is not part of the directory.', 16, 6
  END

  IF @CleanupTime IS NOT NULL AND @CopyOnly = 'Y' AND ((@DirectoryStructure NOT LIKE '%{CopyOnly}%' OR @DirectoryStructure IS NULL) OR (SERVERPROPERTY('IsHadrEnabled') = 1 AND (@AvailabilityGroupDirectoryStructure NOT LIKE '%{CopyOnly}%' OR @AvailabilityGroupDirectoryStructure IS NULL)))
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @CleanupTime is not supported. Cleanup is not supported if the token {CopyOnly} is not part of the directory.', 16, 7
  END

  ----------------------------------------------------------------------------------------------------

  IF @CleanupMode NOT IN('BEFORE_BACKUP','AFTER_BACKUP') OR @CleanupMode IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @CleanupMode is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @Compress NOT IN ('Y','N') OR @Compress IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Compress is not supported.', 16, 1
  END

  IF @Compress = 'Y' AND @BackupSoftware IS NULL AND NOT ((@Version >= 10 AND @Version < 10.5 AND SERVERPROPERTY('EngineEdition') = 3) OR (@Version >= 10.5 AND (SERVERPROPERTY('EngineEdition') IN (3, 8) OR SERVERPROPERTY('EditionID') IN (-1534726760, 284895786))))
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Compress is not supported. Backup compression is not supported in this version and edition of SQL Server.', 16, 2
  END

  IF @Compress = 'N' AND @BackupSoftware IN ('LITESPEED','SQLBACKUP','SQLSAFE') AND (@CompressionLevel IS NULL OR @CompressionLevel >= 1)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Compress is not supported.', 16, 3
  END

  IF @Compress = 'Y' AND @BackupSoftware IN ('LITESPEED','SQLBACKUP','SQLSAFE') AND @CompressionLevel = 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Compress is not supported.', 16, 4
  END

  ----------------------------------------------------------------------------------------------------

  IF @CopyOnly NOT IN ('Y','N') OR @CopyOnly IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @CopyOnly is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @ChangeBackupType NOT IN ('Y','N') OR @ChangeBackupType IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @ChangeBackupType is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @BackupSoftware NOT IN ('LITESPEED','SQLBACKUP','SQLSAFE','DATA_DOMAIN_BOOST')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @BackupSoftware is not supported.', 16, 1
  END

  IF @BackupSoftware IS NOT NULL AND @HostPlatform = 'Linux'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @BackupSoftware is not supported. Only native backups are supported on Linux', 16, 2
  END

  IF @BackupSoftware = 'LITESPEED' AND NOT EXISTS (SELECT * FROM [master].sys.objects WHERE [type] = 'X' AND [name] = 'xp_backup_database')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'LiteSpeed for SQL Server is not installed. Contact your Ascent Technology Consultant. https://www.ascent.tech ', 16, 3
  END

  IF @BackupSoftware = 'SQLBACKUP' AND NOT EXISTS (SELECT * FROM [master].sys.objects WHERE [type] = 'X' AND [name] = 'sqlbackup')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'Red Gate SQL Backup Pro is not installed. Contact your Ascent Technology Consultant. https://www.ascent.tech ', 16, 4
  END

  IF @BackupSoftware = 'SQLSAFE' AND NOT EXISTS (SELECT * FROM [master].sys.objects WHERE [type] = 'X' AND [name] = 'xp_ss_backup')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'Idera SQL Safe Backup is not installed. Contact your Ascent Technology Consultant. https://www.ascent.tech ', 16, 5
  END

  IF @BackupSoftware = 'DATA_DOMAIN_BOOST' AND NOT EXISTS (SELECT * FROM [master].sys.objects WHERE [type] = 'PC' AND [name] = 'emc_run_backup')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'EMC Data Domain Boost is not installed. Download Contact your Ascent Technology Consultant. https://www.ascent.tech ', 16, 6
  END

  ----------------------------------------------------------------------------------------------------

  IF @CheckSum NOT IN ('Y','N') OR @CheckSum IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @CheckSum is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @BlockSize NOT IN (512,1024,2048,4096,8192,16384,32768,65536)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @BlockSize is not supported.', 16, 1
  END

  IF @BlockSize IS NOT NULL AND @BackupSoftware = 'SQLBACKUP'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @BlockSize is not supported. This parameter is not supported with Redgate SQL Backup Pro', 16, 2
  END

  IF @BlockSize IS NOT NULL AND @BackupSoftware = 'SQLSAFE'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @BlockSize is not supported. This parameter is not supported with Idera SQL Safe', 16, 3
  END

  IF @BlockSize IS NOT NULL AND @URL IS NOT NULL AND @Credential IS NOT NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'BLOCKSIZE is not supported when backing up to URL with page blobs. See https://docs.microsoft.com/en-us/sql/relational-databases/backup-restore/sql-server-backup-to-url', 16, 4
  END

  IF @BlockSize IS NOT NULL AND @BackupSoftware = 'DATA_DOMAIN_BOOST'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @BlockSize is not supported. This parameter is not supported with Data Domain Boost', 16, 5
  END

  ----------------------------------------------------------------------------------------------------

  IF @BufferCount <= 0 OR @BufferCount > 2147483647
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @BufferCount is not supported.', 16, 1
  END

  IF @BufferCount IS NOT NULL AND @BackupSoftware = 'SQLBACKUP'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @BufferCount is not supported.', 16, 2
  END

  IF @BufferCount IS NOT NULL AND @BackupSoftware = 'SQLSAFE'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @BufferCount is not supported.', 16, 3
  END

  ----------------------------------------------------------------------------------------------------

  IF @MaxTransferSize < 65536 OR @MaxTransferSize > 4194304
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @MaxTransferSize is not supported.', 16, 1
  END

  IF @MaxTransferSize > 1048576 AND @BackupSoftware = 'SQLBACKUP'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @MaxTransferSize is not supported.', 16, 2
  END

  IF @MaxTransferSize IS NOT NULL AND @BackupSoftware = 'SQLSAFE'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @MaxTransferSize is not supported.', 16, 3
  END

  IF @MaxTransferSize IS NOT NULL AND @URL IS NOT NULL AND @Credential IS NOT NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'MAXTRANSFERSIZE is not supported when backing up to URL with page blobs. See https://docs.microsoft.com/en-us/sql/relational-databases/backup-restore/sql-server-backup-to-url', 16, 4
  END

  IF @MaxTransferSize IS NOT NULL AND @BackupSoftware = 'DATA_DOMAIN_BOOST'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @MaxTransferSize is not supported.', 16, 5
  END

  ----------------------------------------------------------------------------------------------------

  IF @NumberOfFiles < 1 OR @NumberOfFiles > 64
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @NumberOfFiles is not supported.', 16, 1
  END

  IF @NumberOfFiles > 32 AND @BackupSoftware = 'SQLBACKUP'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @NumberOfFiles is not supported.', 16, 2
  END

  IF @NumberOfFiles < (SELECT COUNT(*) FROM @Directories WHERE Mirror = 0)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @NumberOfFiles is not supported.', 16, 3
  END

  IF @NumberOfFiles % (SELECT NULLIF(COUNT(*),0) FROM @Directories WHERE Mirror = 0) > 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @NumberOfFiles is not supported.', 16, 4
  END

  IF @URL IS NOT NULL AND @Credential IS NOT NULL AND @NumberOfFiles <> 1
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'Backup striping to URL with page blobs is not supported. See https://docs.microsoft.com/en-us/sql/relational-databases/backup-restore/sql-server-backup-to-url', 16, 5
  END

  IF @NumberOfFiles > 1 AND @BackupSoftware IN('SQLBACKUP','SQLSAFE') AND EXISTS(SELECT * FROM @Directories WHERE Mirror = 1)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @NumberOfFiles is not supported.', 16, 6
  END

  IF @NumberOfFiles > 32 AND @BackupSoftware = 'DATA_DOMAIN_BOOST'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @NumberOfFiles is not supported.', 16, 7
  END

  IF @NumberOfFiles < (SELECT COUNT(*) FROM @URLs WHERE Mirror = 0)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @NumberOfFiles is not supported.', 16, 8
  END

  IF @NumberOfFiles % (SELECT NULLIF(COUNT(*),0) FROM @URLs WHERE Mirror = 0) > 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @NumberOfFiles is not supported.', 16, 9
  END

    ----------------------------------------------------------------------------------------------------

  IF @MinBackupSizeForMultipleFiles <= 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @MinBackupSizeForMultipleFiles is not supported.', 16, 1
  END

  IF @MinBackupSizeForMultipleFiles IS NOT NULL AND @NumberOfFiles IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @MinBackupSizeForMultipleFiles is not supported. This parameter can only be used together with @NumberOfFiles.', 16, 2
  END

  IF @MinBackupSizeForMultipleFiles IS NOT NULL AND @BackupType = 'DIFF' AND NOT EXISTS(SELECT * FROM sys.all_columns WHERE object_id = OBJECT_ID('sys.dm_db_file_space_usage') AND name = 'modified_extent_page_count')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @MinBackupSizeForMultipleFiles is not supported. The column sys.dm_db_file_space_usage.modified_extent_page_count is not available in this version of SQL Server.', 16, 3
  END

  IF @MinBackupSizeForMultipleFiles IS NOT NULL AND @BackupType = 'LOG' AND NOT EXISTS(SELECT * FROM sys.all_columns WHERE object_id = OBJECT_ID('sys.dm_db_log_stats') AND name = 'log_since_last_log_backup_mb')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @MinBackupSizeForMultipleFiles is not supported. The column sys.dm_db_log_stats.log_since_last_log_backup_mb is not available in this version of SQL Server.', 16, 4
  END

  ----------------------------------------------------------------------------------------------------

  IF @MaxFileSize <= 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @MaxFileSize is not supported.', 16, 1
  END

  IF @MaxFileSize IS NOT NULL AND @NumberOfFiles IS NOT NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The parameters @MaxFileSize and @NumberOfFiles cannot be used together.', 16, 2
  END

  IF @MaxFileSize IS NOT NULL AND @BackupType = 'DIFF' AND NOT EXISTS(SELECT * FROM sys.all_columns WHERE object_id = OBJECT_ID('sys.dm_db_file_space_usage') AND name = 'modified_extent_page_count')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @MaxFileSize is not supported. The column sys.dm_db_file_space_usage.modified_extent_page_count is not available in this version of SQL Server.', 16, 3
  END

  IF @MaxFileSize IS NOT NULL AND @BackupType = 'LOG' AND NOT EXISTS(SELECT * FROM sys.all_columns WHERE object_id = OBJECT_ID('sys.dm_db_log_stats') AND name = 'log_since_last_log_backup_mb')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @MaxFileSize is not supported. The column sys.dm_db_log_stats.log_since_last_log_backup_mb is not available in this version of SQL Server.', 16, 4
  END

  ----------------------------------------------------------------------------------------------------

  IF (@BackupSoftware IS NULL AND @CompressionLevel IS NOT NULL)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @CompressionLevel is not supported.', 16, 1
  END

  IF @BackupSoftware = 'LITESPEED' AND (@CompressionLevel < 0  OR @CompressionLevel > 8)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @CompressionLevel is not supported.', 16, 2
  END

  IF @BackupSoftware = 'SQLBACKUP' AND (@CompressionLevel < 0 OR @CompressionLevel > 4)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @CompressionLevel is not supported.', 16, 3
  END

  IF @BackupSoftware = 'SQLSAFE' AND (@CompressionLevel < 1 OR @CompressionLevel > 4)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @CompressionLevel is not supported.', 16, 4
  END

  IF @CompressionLevel IS NOT NULL AND @BackupSoftware = 'DATA_DOMAIN_BOOST'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @CompressionLevel is not supported.', 16, 5
  END

  ----------------------------------------------------------------------------------------------------

  IF LEN(@Description) > 255
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Description is not supported.', 16, 1
  END

  IF @BackupSoftware = 'LITESPEED' AND LEN(@Description) > 128
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Description is not supported.', 16, 2
  END

  IF @BackupSoftware = 'DATA_DOMAIN_BOOST' AND LEN(@Description) > 254
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Description is not supported.', 16, 3
  END

  ----------------------------------------------------------------------------------------------------

  IF @Threads IS NOT NULL AND (@BackupSoftware NOT IN('LITESPEED','SQLBACKUP','SQLSAFE') OR @BackupSoftware IS NULL)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Threads is not supported.', 16, 1
  END

  IF @BackupSoftware = 'LITESPEED' AND (@Threads < 1 OR @Threads > 32)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Threads is not supported.', 16, 2
  END

  IF @BackupSoftware = 'SQLBACKUP' AND (@Threads < 2 OR @Threads > 32)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Threads is not supported.', 16, 3
  END

  IF @BackupSoftware = 'SQLSAFE' AND (@Threads < 1 OR @Threads > 64)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Threads is not supported.', 16, 4
  END

  ----------------------------------------------------------------------------------------------------

  IF @Throttle < 1 OR @Throttle > 100
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Throttle is not supported.', 16, 1
  END

  IF @Throttle IS NOT NULL AND (@BackupSoftware NOT IN('LITESPEED') OR @BackupSoftware IS NULL)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Throttle is not supported.', 16, 2
  END

  ----------------------------------------------------------------------------------------------------

  IF @Encrypt NOT IN('Y','N') OR @Encrypt IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Encrypt is not supported.', 16, 1
  END

  IF @Encrypt = 'Y' AND @BackupSoftware IS NULL AND NOT (@Version >= 12 AND (SERVERPROPERTY('EngineEdition') = 3) OR SERVERPROPERTY('EditionID') IN(-1534726760, 284895786))
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Encrypt is not supported.', 16, 2
  END

  IF @Encrypt = 'Y' AND @BackupSoftware = 'DATA_DOMAIN_BOOST'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Encrypt is not supported.', 16, 3
  END

  ----------------------------------------------------------------------------------------------------

  IF @BackupSoftware IS NULL AND @Encrypt = 'Y' AND (@EncryptionAlgorithm NOT IN('AES_128','AES_192','AES_256','TRIPLE_DES_3KEY') OR @EncryptionAlgorithm IS NULL)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @EncryptionAlgorithm is not supported.', 16, 1
  END

  IF @BackupSoftware = 'LITESPEED' AND @Encrypt = 'Y' AND (@EncryptionAlgorithm NOT IN('RC2_40','RC2_56','RC2_112','RC2_128','TRIPLE_DES_3KEY','RC4_128','AES_128','AES_192','AES_256') OR @EncryptionAlgorithm IS NULL)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @EncryptionAlgorithm is not supported.', 16, 2
  END

  IF @BackupSoftware = 'SQLBACKUP' AND @Encrypt = 'Y' AND (@EncryptionAlgorithm NOT IN('AES_128','AES_256') OR @EncryptionAlgorithm IS NULL)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @EncryptionAlgorithm is not supported.', 16, 3
  END

  IF @BackupSoftware = 'SQLSAFE' AND @Encrypt = 'Y' AND (@EncryptionAlgorithm NOT IN('AES_128','AES_256') OR @EncryptionAlgorithm IS NULL)
  OR (@EncryptionAlgorithm IS NOT NULL AND @BackupSoftware = 'DATA_DOMAIN_BOOST')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @EncryptionAlgorithm is not supported.', 16, 4
  END

  IF @EncryptionAlgorithm IS NOT NULL AND @BackupSoftware = 'DATA_DOMAIN_BOOST'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @EncryptionAlgorithm is not supported.', 16, 5
  END

  ----------------------------------------------------------------------------------------------------

  IF (NOT (@BackupSoftware IS NULL AND @Encrypt = 'Y') AND @ServerCertificate IS NOT NULL)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @ServerCertificate is not supported.', 16, 1
  END

  IF @BackupSoftware IS NULL AND @Encrypt = 'Y' AND @ServerCertificate IS NULL AND @ServerAsymmetricKey IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @ServerCertificate is not supported.', 16, 2
  END

  IF @BackupSoftware IS NULL AND @Encrypt = 'Y' AND @ServerCertificate IS NOT NULL AND @ServerAsymmetricKey IS NOT NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @ServerCertificate is not supported.', 16, 3
  END

  IF @ServerCertificate IS NOT NULL AND NOT EXISTS(SELECT * FROM master.sys.certificates WHERE name = @ServerCertificate)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @ServerCertificate is not supported.', 16, 4
  END

  ----------------------------------------------------------------------------------------------------

  IF NOT (@BackupSoftware IS NULL AND @Encrypt = 'Y') AND @ServerAsymmetricKey IS NOT NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @ServerAsymmetricKey is not supported.', 16, 1
  END

  IF @BackupSoftware IS NULL AND @Encrypt = 'Y' AND @ServerAsymmetricKey IS NULL AND @ServerCertificate IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @ServerAsymmetricKey is not supported.', 16, 2
  END

  IF @BackupSoftware IS NULL AND @Encrypt = 'Y' AND @ServerAsymmetricKey IS NOT NULL AND @ServerCertificate IS NOT NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @ServerAsymmetricKey is not supported.', 16, 3
  END

  IF @ServerAsymmetricKey IS NOT NULL AND NOT EXISTS(SELECT * FROM master.sys.asymmetric_keys WHERE name = @ServerAsymmetricKey)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @ServerAsymmetricKey is not supported.', 16, 4
  END

  ----------------------------------------------------------------------------------------------------

  IF @EncryptionKey IS NOT NULL AND @BackupSoftware IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @EncryptionKey is not supported.', 16, 1
  END

  IF @EncryptionKey IS NOT NULL AND @Encrypt = 'N'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @EncryptionKey is not supported.', 16, 2
  END

  IF @EncryptionKey IS NULL AND @Encrypt = 'Y' AND @BackupSoftware IN('LITESPEED','SQLBACKUP','SQLSAFE')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @EncryptionKey is not supported.', 16, 3
  END

  IF @EncryptionKey IS NOT NULL AND @BackupSoftware = 'DATA_DOMAIN_BOOST'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @EncryptionKey is not supported.', 16, 4
  END

  ----------------------------------------------------------------------------------------------------

  IF @ReadWriteFileGroups NOT IN('Y','N') OR @ReadWriteFileGroups IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @ReadWriteFileGroups is not supported.', 16, 1
  END

  IF @ReadWriteFileGroups = 'Y' AND @BackupType = 'LOG'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @ReadWriteFileGroups is not supported.', 16, 2
  END

  ----------------------------------------------------------------------------------------------------

  IF @OverrideBackupPreference NOT IN('Y','N') OR @OverrideBackupPreference IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @OverrideBackupPreference is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @NoRecovery NOT IN('Y','N') OR @NoRecovery IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @NoRecovery is not supported.', 16, 1
  END

  IF @NoRecovery = 'Y' AND @BackupType <> 'LOG'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @NoRecovery is not supported.', 16, 2
  END

  IF @NoRecovery = 'Y' AND @BackupSoftware = 'SQLSAFE'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @NoRecovery is not supported.', 16, 3
  END

  ----------------------------------------------------------------------------------------------------

  IF @URL IS NOT NULL AND @Directory IS NOT NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @URL is not supported.', 16, 1
  END

  IF @URL IS NOT NULL AND @MirrorDirectory IS NOT NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @URL is not supported.', 16, 2
  END

  IF @URL IS NOT NULL AND @Version < 11.03339
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @URL is not supported.', 16, 3
  END

  IF @URL IS NOT NULL AND @BackupSoftware IS NOT NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @URL is not supported.', 16, 4
  END

  ----------------------------------------------------------------------------------------------------

  IF @Credential IS NULL AND @URL IS NOT NULL AND NOT (@Version >= 13 OR SERVERPROPERTY('EngineEdition') = 8)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Credential is not supported.', 16, 1
  END

  IF @Credential IS NOT NULL AND @URL IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Credential is not supported.', 16, 2
  END

  IF @URL IS NOT NULL AND @Credential IS NULL AND NOT EXISTS(SELECT * FROM sys.credentials WHERE UPPER(credential_identity) = 'SHARED ACCESS SIGNATURE')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Credential is not supported.', 16, 3
  END

  IF @Credential IS NOT NULL AND NOT EXISTS(SELECT * FROM sys.credentials WHERE name = @Credential)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Credential is not supported.', 16, 4
  END

  ----------------------------------------------------------------------------------------------------

  IF @MirrorCleanupTime < 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @MirrorCleanupTime is not supported.', 16, 1
  END

  IF @MirrorCleanupTime IS NOT NULL AND @MirrorDirectory IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @MirrorCleanupTime is not supported.', 16, 2
  END

  ----------------------------------------------------------------------------------------------------

  IF @MirrorCleanupMode NOT IN('BEFORE_BACKUP','AFTER_BACKUP') OR @MirrorCleanupMode IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @MirrorCleanupMode is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @MirrorURL IS NOT NULL AND @Directory IS NOT NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @MirrorURL is not supported.', 16, 1
  END

  IF @MirrorURL IS NOT NULL AND @MirrorDirectory IS NOT NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @MirrorURL is not supported.', 16, 2
  END

  IF @MirrorURL IS NOT NULL AND @Version < 11.03339
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @MirrorURL is not supported.', 16, 3
  END

  IF @MirrorURL IS NOT NULL AND @BackupSoftware IS NOT NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @MirrorURL is not supported.', 16, 4
  END

  IF @MirrorURL IS NOT NULL AND @URL IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @MirrorURL is not supported.', 16, 5
  END

  ----------------------------------------------------------------------------------------------------

  IF @Updateability NOT IN('READ_ONLY','READ_WRITE','ALL') OR @Updateability IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Updateability is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @AdaptiveCompression NOT IN('SIZE','SPEED')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @AdaptiveCompression is not supported.', 16, 1
  END

  IF @AdaptiveCompression IS NOT NULL AND (@BackupSoftware NOT IN('LITESPEED') OR @BackupSoftware IS NULL)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @AdaptiveCompression is not supported.', 16, 2
  END

  ----------------------------------------------------------------------------------------------------

  IF @ModificationLevel IS NOT NULL AND NOT EXISTS(SELECT * FROM sys.all_columns WHERE object_id = OBJECT_ID('sys.dm_db_file_space_usage') AND name = 'modified_extent_page_count')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @ModificationLevel is not supported.', 16, 1
  END

  IF @ModificationLevel IS NOT NULL AND @ChangeBackupType = 'N'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The parameter @ModificationLevel can only be used together with @ChangeBackupType = ''Y''.', 16, 2
  END

  IF @ModificationLevel IS NOT NULL AND @BackupType <> 'DIFF'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The parameter @ModificationLevel can only be used for differential backups.', 16, 3
  END

  ----------------------------------------------------------------------------------------------------

  IF @LogSizeSinceLastLogBackup IS NOT NULL AND NOT EXISTS(SELECT * FROM sys.all_columns WHERE object_id = OBJECT_ID('sys.dm_db_log_stats') AND name = 'log_since_last_log_backup_mb')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @LogSizeSinceLastLogBackup is not supported.', 16, 1
  END

  IF @LogSizeSinceLastLogBackup IS NOT NULL AND @BackupType <> 'LOG'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @LogSizeSinceLastLogBackup is not supported.', 16, 2
  END

  ----------------------------------------------------------------------------------------------------

  IF @TimeSinceLastLogBackup IS NOT NULL AND NOT EXISTS(SELECT * FROM sys.all_columns WHERE object_id = OBJECT_ID('sys.dm_db_log_stats') AND name = 'log_backup_time')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @TimeSinceLastLogBackup is not supported.', 16, 1
  END

  IF @TimeSinceLastLogBackup IS NOT NULL AND @BackupType <> 'LOG'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @TimeSinceLastLogBackup is not supported.', 16, 2
  END

  ----------------------------------------------------------------------------------------------------

  IF (@TimeSinceLastLogBackup IS NOT NULL AND @LogSizeSinceLastLogBackup IS NULL) OR (@TimeSinceLastLogBackup IS NULL AND @LogSizeSinceLastLogBackup IS NOT NULL)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The parameters @TimeSinceLastLogBackup and @LogSizeSinceLastLogBackup can only be used together.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @DataDomainBoostHost IS NOT NULL AND (@BackupSoftware <> 'DATA_DOMAIN_BOOST' OR @BackupSoftware IS NULL)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @DataDomainBoostHost is not supported.', 16, 1
  END

  IF @DataDomainBoostHost IS NULL AND @BackupSoftware = 'DATA_DOMAIN_BOOST'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @DataDomainBoostHost is not supported.', 16, 2
  END

  ----------------------------------------------------------------------------------------------------

  IF @DataDomainBoostUser IS NOT NULL AND (@BackupSoftware <> 'DATA_DOMAIN_BOOST' OR @BackupSoftware IS NULL)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @DataDomainBoostUser is not supported.', 16, 1
  END

  IF @DataDomainBoostUser IS NULL AND @BackupSoftware = 'DATA_DOMAIN_BOOST'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @DataDomainBoostUser is not supported.', 16, 2
  END

  ----------------------------------------------------------------------------------------------------

  IF @DataDomainBoostDevicePath IS NOT NULL AND (@BackupSoftware <> 'DATA_DOMAIN_BOOST' OR @BackupSoftware IS NULL)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @DataDomainBoostDevicePath is not supported.', 16, 1
  END

  IF @DataDomainBoostDevicePath IS NULL AND @BackupSoftware = 'DATA_DOMAIN_BOOST'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @DataDomainBoostDevicePath is not supported.', 16, 2
  END

  ----------------------------------------------------------------------------------------------------

  IF @DataDomainBoostLockboxPath IS NOT NULL AND (@BackupSoftware <> 'DATA_DOMAIN_BOOST' OR @BackupSoftware IS NULL)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @DataDomainBoostLockboxPath is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @DirectoryStructure = ''
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @DirectoryStructure is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @AvailabilityGroupDirectoryStructure = ''
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @AvailabilityGroupDirectoryStructure is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @FileName IS NULL OR @FileName = ''
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @FileName is not supported.', 16, 1
  END

  IF @FileName NOT LIKE '%.{FileExtension}'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @FileName is not supported.', 16, 2
  END

  IF (@NumberOfFiles > 1 AND @FileName NOT LIKE '%{FileNumber}%')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @FileName is not supported.', 16, 3
  END

  IF @FileName LIKE '%{DirectorySeparator}%'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @FileName is not supported.', 16, 4
  END

  IF @FileName LIKE '%/%'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @FileName is not supported.', 16, 5
  END

  IF @FileName LIKE '%\%'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @FileName is not supported.', 16, 6
  END

  ----------------------------------------------------------------------------------------------------

  IF (SERVERPROPERTY('IsHadrEnabled') = 1 AND @AvailabilityGroupFileName IS NULL)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @AvailabilityGroupFileName is not supported.', 16, 1
  END

  IF @AvailabilityGroupFileName = ''
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @AvailabilityGroupFileName is not supported.', 16, 2
  END

  IF @AvailabilityGroupFileName NOT LIKE '%.{FileExtension}'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @AvailabilityGroupFileName is not supported.', 16, 3
  END

  IF (@NumberOfFiles > 1 AND @AvailabilityGroupFileName NOT LIKE '%{FileNumber}%')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @AvailabilityGroupFileName is not supported.', 16, 4
  END

  IF @AvailabilityGroupFileName LIKE '%{DirectorySeparator}%'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @AvailabilityGroupFileName is not supported.', 16, 5
  END

  IF @AvailabilityGroupFileName LIKE '%/%'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @AvailabilityGroupFileName is not supported.', 16, 6
  END

  IF @AvailabilityGroupFileName LIKE '%\%'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @AvailabilityGroupFileName is not supported.', 16, 7
  END

  ----------------------------------------------------------------------------------------------------

  IF EXISTS (SELECT * FROM (SELECT REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@DirectoryStructure,'{DirectorySeparator}',''),'{ServerName}',''),'{InstanceName}',''),'{ServiceName}',''),'{ClusterName}',''),'{AvailabilityGroupName}',''),'{DatabaseName}',''),'{BackupType}',''),'{Partial}',''),'{CopyOnly}',''),'{Description}',''),'{Year}',''),'{Month}',''),'{Day}',''),'{Week}',''),'{Hour}',''),'{Minute}',''),'{Second}',''),'{Millisecond}',''),'{Microsecond}',''),'{MajorVersion}',''),'{MinorVersion}','') AS DirectoryStructure) Temp WHERE DirectoryStructure LIKE '%{%' OR DirectoryStructure LIKE '%}%')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The parameter @DirectoryStructure contains one or more tokens that are not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF EXISTS (SELECT * FROM (SELECT REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@AvailabilityGroupDirectoryStructure,'{DirectorySeparator}',''),'{ServerName}',''),'{InstanceName}',''),'{ServiceName}',''),'{ClusterName}',''),'{AvailabilityGroupName}',''),'{DatabaseName}',''),'{BackupType}',''),'{Partial}',''),'{CopyOnly}',''),'{Description}',''),'{Year}',''),'{Month}',''),'{Day}',''),'{Week}',''),'{Hour}',''),'{Minute}',''),'{Second}',''),'{Millisecond}',''),'{Microsecond}',''),'{MajorVersion}',''),'{MinorVersion}','') AS AvailabilityGroupDirectoryStructure) Temp WHERE AvailabilityGroupDirectoryStructure LIKE '%{%' OR AvailabilityGroupDirectoryStructure LIKE '%}%')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The parameter @AvailabilityGroupDirectoryStructure contains one or more tokens that are not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF EXISTS (SELECT * FROM (SELECT REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@FileName,'{DirectorySeparator}',''),'{ServerName}',''),'{InstanceName}',''),'{ServiceName}',''),'{ClusterName}',''),'{AvailabilityGroupName}',''),'{DatabaseName}',''),'{BackupType}',''),'{Partial}',''),'{CopyOnly}',''),'{Description}',''),'{Year}',''),'{Month}',''),'{Day}',''),'{Week}',''),'{Hour}',''),'{Minute}',''),'{Second}',''),'{Millisecond}',''),'{Microsecond}',''),'{FileNumber}',''),'{NumberOfFiles}',''),'{FileExtension}',''),'{MajorVersion}',''),'{MinorVersion}','') AS [FileName]) Temp WHERE [FileName] LIKE '%{%' OR [FileName] LIKE '%}%')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The parameter @FileName contains one or more tokens that are not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF EXISTS (SELECT * FROM (SELECT REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@AvailabilityGroupFileName,'{DirectorySeparator}',''),'{ServerName}',''),'{InstanceName}',''),'{ServiceName}',''),'{ClusterName}',''),'{AvailabilityGroupName}',''),'{DatabaseName}',''),'{BackupType}',''),'{Partial}',''),'{CopyOnly}',''),'{Description}',''),'{Year}',''),'{Month}',''),'{Day}',''),'{Week}',''),'{Hour}',''),'{Minute}',''),'{Second}',''),'{Millisecond}',''),'{Microsecond}',''),'{FileNumber}',''),'{NumberOfFiles}',''),'{FileExtension}',''),'{MajorVersion}',''),'{MinorVersion}','') AS AvailabilityGroupFileName) Temp WHERE AvailabilityGroupFileName LIKE '%{%' OR AvailabilityGroupFileName LIKE '%}%')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The parameter @AvailabilityGroupFileName contains one or more tokens that are not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @FileExtensionFull LIKE '%.%'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @FileExtensionFull is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @FileExtensionDiff LIKE '%.%'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @FileExtensionDiff is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @FileExtensionLog LIKE '%.%'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @FileExtensionLog is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @Init NOT IN('Y','N') OR @Init IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Init is not supported.', 16, 1
  END

  IF @Init = 'Y' AND @BackupType = 'LOG'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Init is not supported.', 16, 2
  END

  IF @Init = 'Y' AND @BackupSoftware = 'DATA_DOMAIN_BOOST'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Init is not supported.', 16, 3
  END

  ----------------------------------------------------------------------------------------------------

  IF @Format NOT IN('Y','N') OR @Format IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Format is not supported.', 16, 1
  END

  IF @Format = 'Y' AND @BackupType = 'LOG'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Format is not supported.', 16, 2
  END

  IF @Format = 'Y' AND @BackupSoftware = 'DATA_DOMAIN_BOOST'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Format is not supported.', 16, 3
  END

  ----------------------------------------------------------------------------------------------------

  IF @ObjectLevelRecoveryMap NOT IN('Y','N') OR @ObjectLevelRecoveryMap IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @ObjectLevelRecovery is not supported.', 16, 1
  END

  IF @ObjectLevelRecoveryMap = 'Y' AND @BackupSoftware IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @ObjectLevelRecovery is not supported.', 16, 2
  END

  IF @ObjectLevelRecoveryMap = 'Y' AND @BackupSoftware <> 'LITESPEED'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @ObjectLevelRecovery is not supported.', 16, 3
  END

  IF @ObjectLevelRecoveryMap = 'Y' AND @BackupType = 'LOG'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @ObjectLevelRecovery is not supported.', 16, 4
  END

  ----------------------------------------------------------------------------------------------------

  IF @ExcludeLogShippedFromLogBackup NOT IN('Y','N') OR @ExcludeLogShippedFromLogBackup IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @ExcludeLogShippedFromLogBackup is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @DirectoryCheck NOT IN('Y','N') OR @DirectoryCheck IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @DirectoryCheck is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @StringDelimiter IS NULL OR LEN(@StringDelimiter) > 1
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @StringDelimiter is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @DatabaseOrder NOT IN('DATABASE_NAME_ASC','DATABASE_NAME_DESC','DATABASE_SIZE_ASC','DATABASE_SIZE_DESC','LOG_SIZE_SINCE_LAST_LOG_BACKUP_ASC','LOG_SIZE_SINCE_LAST_LOG_BACKUP_DESC')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @DatabaseOrder is not supported.', 16, 1
  END

  IF @DatabaseOrder IN('LOG_SIZE_SINCE_LAST_LOG_BACKUP_ASC','LOG_SIZE_SINCE_LAST_LOG_BACKUP_DESC') AND NOT EXISTS(SELECT * FROM sys.all_columns WHERE object_id = OBJECT_ID('sys.dm_db_log_stats') AND name = 'log_since_last_log_backup_mb')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @DatabaseOrder is not supported. The column sys.dm_db_log_stats.log_since_last_log_backup_mb is not available in this version of SQL Server.', 16, 2
  END

  IF @DatabaseOrder IS NOT NULL AND SERVERPROPERTY('EngineEdition') = 5
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @DatabaseOrder is not supported.', 16, 3
  END

  ----------------------------------------------------------------------------------------------------

  IF @DatabasesInParallel NOT IN('Y','N') OR @DatabasesInParallel IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @DatabasesInParallel is not supported.', 16, 1
  END

  IF @DatabasesInParallel = 'Y' AND SERVERPROPERTY('EngineEdition') = 5
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @DatabasesInParallel is not supported.', 16, 2
  END

  ----------------------------------------------------------------------------------------------------

  IF @LogToTable NOT IN('Y','N') OR @LogToTable IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @LogToTable is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @Execute NOT IN('Y','N') OR @Execute IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Execute is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF EXISTS(SELECT * FROM @Errors)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The documentation is available from your Ascent Technology Consultant. https://www.ascent.tech ', 16, 1
  END

  ----------------------------------------------------------------------------------------------------
  --// Check that selected databases and availability groups exist                                //--
  ----------------------------------------------------------------------------------------------------

  SET @ErrorMessage = ''
  SELECT @ErrorMessage = @ErrorMessage + QUOTENAME(DatabaseName) + ', '
  FROM @SelectedDatabases
  WHERE DatabaseName NOT LIKE '%[%]%'
  AND DatabaseName NOT IN (SELECT DatabaseName FROM @tmpDatabases)
  IF @@ROWCOUNT > 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The following databases in the @Databases parameter do not exist: ' + LEFT(@ErrorMessage,LEN(@ErrorMessage)-1) + '.', 10, 1
  END

  SET @ErrorMessage = ''
  SELECT @ErrorMessage = @ErrorMessage + QUOTENAME(AvailabilityGroupName) + ', '
  FROM @SelectedAvailabilityGroups
  WHERE AvailabilityGroupName NOT LIKE '%[%]%'
  AND AvailabilityGroupName NOT IN (SELECT AvailabilityGroupName FROM @tmpAvailabilityGroups)
  IF @@ROWCOUNT > 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The following availability groups do not exist: ' + LEFT(@ErrorMessage,LEN(@ErrorMessage)-1) + '.', 10, 1
  END

  ----------------------------------------------------------------------------------------------------
  --// Check @@SERVERNAME                                                                         //--
  ----------------------------------------------------------------------------------------------------

  IF @@SERVERNAME <> CAST(SERVERPROPERTY('ServerName') AS nvarchar(max)) AND SERVERPROPERTY('IsHadrEnabled') = 1
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The @@SERVERNAME does not match SERVERPROPERTY(''ServerName''). See ' + CASE WHEN SERVERPROPERTY('IsClustered') = 0 THEN 'https://docs.microsoft.com/en-us/sql/database-engine/install-windows/rename-a-computer-that-hosts-a-stand-alone-instance-of-sql-server' WHEN SERVERPROPERTY('IsClustered') = 1 THEN 'https://docs.microsoft.com/en-us/sql/sql-server/failover-clusters/install/rename-a-sql-server-failover-cluster-instance' END + '.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------
  --// Raise errors                                                                               //--
  ----------------------------------------------------------------------------------------------------

  DECLARE ErrorCursor CURSOR FAST_FORWARD FOR SELECT [Message], Severity, [State] FROM @Errors ORDER BY [ID] ASC

  OPEN ErrorCursor

  FETCH ErrorCursor INTO @CurrentMessage, @CurrentSeverity, @CurrentState

  WHILE @@FETCH_STATUS = 0
  BEGIN
    RAISERROR('%s', @CurrentSeverity, @CurrentState, @CurrentMessage) WITH NOWAIT
    RAISERROR(@EmptyLine, 10, 1) WITH NOWAIT

    FETCH NEXT FROM ErrorCursor INTO @CurrentMessage, @CurrentSeverity, @CurrentState
  END

  CLOSE ErrorCursor

  DEALLOCATE ErrorCursor

  IF EXISTS (SELECT * FROM @Errors WHERE Severity >= 16)
  BEGIN
    SET @ReturnCode = 50000
    GOTO Logging
  END

  ----------------------------------------------------------------------------------------------------
  --// Check Availability Group cluster name                                                      //--
  ----------------------------------------------------------------------------------------------------

  IF @Version >= 11 AND SERVERPROPERTY('IsHadrEnabled') = 1
  BEGIN
    SELECT @Cluster = NULLIF(cluster_name,'')
    FROM sys.dm_hadr_cluster
  END

  ----------------------------------------------------------------------------------------------------
  --// Update database order                                                                      //--
  ----------------------------------------------------------------------------------------------------

  IF @DatabaseOrder IN('DATABASE_SIZE_ASC','DATABASE_SIZE_DESC')
  BEGIN
    UPDATE tmpDatabases
    SET DatabaseSize = (SELECT SUM(CAST(size AS bigint)) FROM sys.master_files WHERE [type] = 0 AND database_id = DB_ID(tmpDatabases.DatabaseName))
    FROM @tmpDatabases tmpDatabases
  END

  IF @DatabaseOrder IN('LOG_SIZE_SINCE_LAST_LOG_BACKUP_ASC','LOG_SIZE_SINCE_LAST_LOG_BACKUP_DESC')
  BEGIN
    UPDATE tmpDatabases
    SET LogSizeSinceLastLogBackup = (SELECT log_since_last_log_backup_mb FROM sys.dm_db_log_stats(DB_ID(tmpDatabases.DatabaseName)))
    FROM @tmpDatabases tmpDatabases
  END

  IF @DatabaseOrder IS NULL
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY StartPosition ASC, DatabaseName ASC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'DATABASE_NAME_ASC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY DatabaseName ASC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'DATABASE_NAME_DESC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY DatabaseName DESC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'DATABASE_SIZE_ASC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY DatabaseSize ASC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'DATABASE_SIZE_DESC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY DatabaseSize DESC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'LOG_SIZE_SINCE_LAST_LOG_BACKUP_ASC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY LogSizeSinceLastLogBackup ASC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'LOG_SIZE_SINCE_LAST_LOG_BACKUP_DESC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY LogSizeSinceLastLogBackup DESC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END

  ----------------------------------------------------------------------------------------------------
  --// Update the queue                                                                           //--
  ----------------------------------------------------------------------------------------------------

  IF @DatabasesInParallel = 'Y'
  BEGIN

    BEGIN TRY

      SELECT @QueueID = QueueID
      FROM dbo.[Queue]
      WHERE SchemaName = @SchemaName
      AND ObjectName = @ObjectName
      AND [Parameters] = @Parameters

      IF @QueueID IS NULL
      BEGIN
        BEGIN TRANSACTION

        SELECT @QueueID = QueueID
        FROM dbo.[Queue] WITH (UPDLOCK, HOLDLOCK)
        WHERE SchemaName = @SchemaName
        AND ObjectName = @ObjectName
        AND [Parameters] = @Parameters

        IF @QueueID IS NULL
        BEGIN
          INSERT INTO dbo.[Queue] (SchemaName, ObjectName, [Parameters])
          SELECT @SchemaName, @ObjectName, @Parameters

          SET @QueueID = SCOPE_IDENTITY()
        END

        COMMIT TRANSACTION
      END

      BEGIN TRANSACTION

      UPDATE [Queue]
      SET QueueStartTime = SYSDATETIME(),
          SessionID = @@SPID,
          RequestID = (SELECT request_id FROM sys.dm_exec_requests WHERE session_id = @@SPID),
          RequestStartTime = (SELECT start_time FROM sys.dm_exec_requests WHERE session_id = @@SPID)
      FROM dbo.[Queue] [Queue]
      WHERE QueueID = @QueueID
      AND NOT EXISTS (SELECT *
                      FROM sys.dm_exec_requests
                      WHERE session_id = [Queue].SessionID
                      AND request_id = [Queue].RequestID
                      AND start_time = [Queue].RequestStartTime)
      AND NOT EXISTS (SELECT *
                      FROM dbo.QueueDatabase QueueDatabase
                      INNER JOIN sys.dm_exec_requests ON QueueDatabase.SessionID = session_id AND QueueDatabase.RequestID = request_id AND QueueDatabase.RequestStartTime = start_time
                      WHERE QueueDatabase.QueueID = @QueueID)

      IF @@ROWCOUNT = 1
      BEGIN
        INSERT INTO dbo.QueueDatabase (QueueID, DatabaseName)
        SELECT @QueueID AS QueueID,
               DatabaseName
        FROM @tmpDatabases tmpDatabases
        WHERE Selected = 1
        AND NOT EXISTS (SELECT * FROM dbo.QueueDatabase WHERE DatabaseName = tmpDatabases.DatabaseName AND QueueID = @QueueID)

        DELETE QueueDatabase
        FROM dbo.QueueDatabase QueueDatabase
        WHERE QueueID = @QueueID
        AND NOT EXISTS (SELECT * FROM @tmpDatabases tmpDatabases WHERE DatabaseName = QueueDatabase.DatabaseName AND Selected = 1)

        UPDATE QueueDatabase
        SET DatabaseOrder = tmpDatabases.[Order]
        FROM dbo.QueueDatabase QueueDatabase
        INNER JOIN @tmpDatabases tmpDatabases ON QueueDatabase.DatabaseName = tmpDatabases.DatabaseName
        WHERE QueueID = @QueueID
      END

      COMMIT TRANSACTION

      SELECT @QueueStartTime = QueueStartTime
      FROM dbo.[Queue]
      WHERE QueueID = @QueueID

    END TRY

    BEGIN CATCH
      IF XACT_STATE() <> 0
      BEGIN
        ROLLBACK TRANSACTION
      END
      SET @ErrorMessage = 'Msg ' + CAST(ERROR_NUMBER() AS nvarchar) + ', ' + ISNULL(ERROR_MESSAGE(),'')
      RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
      RAISERROR(@EmptyLine,10,1) WITH NOWAIT
      SET @ReturnCode = ERROR_NUMBER()
      GOTO Logging
    END CATCH

  END

  ----------------------------------------------------------------------------------------------------
  --// Execute backup commands                                                                    //--
  ----------------------------------------------------------------------------------------------------

  WHILE (1 = 1)
  BEGIN

    IF @DatabasesInParallel = 'Y'
    BEGIN
      UPDATE QueueDatabase
      SET DatabaseStartTime = NULL,
          SessionID = NULL,
          RequestID = NULL,
          RequestStartTime = NULL
      FROM dbo.QueueDatabase QueueDatabase
      WHERE QueueID = @QueueID
      AND DatabaseStartTime IS NOT NULL
      AND DatabaseEndTime IS NULL
      AND NOT EXISTS (SELECT * FROM sys.dm_exec_requests WHERE session_id = QueueDatabase.SessionID AND request_id = QueueDatabase.RequestID AND start_time = QueueDatabase.RequestStartTime)

      UPDATE QueueDatabase
      SET DatabaseStartTime = SYSDATETIME(),
          DatabaseEndTime = NULL,
          SessionID = @@SPID,
          RequestID = (SELECT request_id FROM sys.dm_exec_requests WHERE session_id = @@SPID),
          RequestStartTime = (SELECT start_time FROM sys.dm_exec_requests WHERE session_id = @@SPID),
          @CurrentDatabaseName = DatabaseName,
          @CurrentDatabaseNameFS = (SELECT DatabaseNameFS FROM @tmpDatabases WHERE DatabaseName = QueueDatabase.DatabaseName)
      FROM (SELECT TOP 1 DatabaseStartTime,
                         DatabaseEndTime,
                         SessionID,
                         RequestID,
                         RequestStartTime,
                         DatabaseName
            FROM dbo.QueueDatabase
            WHERE QueueID = @QueueID
            AND (DatabaseStartTime < @QueueStartTime OR DatabaseStartTime IS NULL)
            AND NOT (DatabaseStartTime IS NOT NULL AND DatabaseEndTime IS NULL)
            ORDER BY DatabaseOrder ASC
            ) QueueDatabase
    END
    ELSE
    BEGIN
      SELECT TOP 1 @CurrentDBID = ID,
                   @CurrentDatabaseName = DatabaseName,
                   @CurrentDatabaseNameFS = DatabaseNameFS
      FROM @tmpDatabases
      WHERE Selected = 1
      AND Completed = 0
      ORDER BY [Order] ASC
    END

    IF @@ROWCOUNT = 0
    BEGIN
      BREAK
    END

    SET @CurrentDatabase_sp_executesql = QUOTENAME(@CurrentDatabaseName) + '.sys.sp_executesql'

    BEGIN
      SET @DatabaseMessage = 'Date and time: ' + CONVERT(nvarchar,SYSDATETIME(),120)
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Database: ' + QUOTENAME(@CurrentDatabaseName)
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
    END

    SELECT @CurrentUserAccess = user_access_desc,
           @CurrentIsReadOnly = is_read_only,
           @CurrentDatabaseState = state_desc,
           @CurrentInStandby = is_in_standby,
           @CurrentRecoveryModel = recovery_model_desc,
           @CurrentIsEncrypted = is_encrypted,
           @CurrentDatabaseSize = (SELECT SUM(CAST(size AS bigint)) FROM sys.master_files WHERE [type] = 0 AND database_id = sys.databases.database_id)
    FROM sys.databases
    WHERE [name] = @CurrentDatabaseName

    BEGIN
      SET @DatabaseMessage = 'State: ' + @CurrentDatabaseState
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Standby: ' + CASE WHEN @CurrentInStandby = 1 THEN 'Yes' ELSE 'No' END
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage =  'Updateability: ' + CASE WHEN @CurrentIsReadOnly = 1 THEN 'READ_ONLY' WHEN  @CurrentIsReadOnly = 0 THEN 'READ_WRITE' END
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage =  'User access: ' + @CurrentUserAccess
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Recovery model: ' + @CurrentRecoveryModel
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Encrypted: ' + CASE WHEN @CurrentIsEncrypted = 1 THEN 'Yes' WHEN @CurrentIsEncrypted = 0 THEN 'No' ELSE 'N/A' END
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
    END

    SELECT @CurrentMaxTransferSize = CASE
    WHEN @MaxTransferSize IS NOT NULL THEN @MaxTransferSize
    WHEN @MaxTransferSize IS NULL AND @Compress = 'Y' AND @CurrentIsEncrypted = 1 AND @BackupSoftware IS NULL AND (@Version >= 13 AND @Version < 15.0404316) AND @Credential IS NULL THEN 65537
    END

    IF @CurrentDatabaseState = 'ONLINE' AND NOT (@CurrentInStandby = 1)
    BEGIN
      IF EXISTS (SELECT * FROM sys.database_recovery_status WHERE database_id = DB_ID(@CurrentDatabaseName) AND database_guid IS NOT NULL)
      BEGIN
        SET @CurrentIsDatabaseAccessible = 1
      END
      ELSE
      BEGIN
        SET @CurrentIsDatabaseAccessible = 0
      END
    END

    IF @Version >= 11 AND SERVERPROPERTY('IsHadrEnabled') = 1
    BEGIN
      SELECT @CurrentReplicaID = databases.replica_id
      FROM sys.databases databases
      INNER JOIN sys.availability_replicas availability_replicas ON databases.replica_id = availability_replicas.replica_id
      WHERE databases.[name] = @CurrentDatabaseName

      SELECT @CurrentAvailabilityGroupID = group_id
      FROM sys.availability_replicas
      WHERE replica_id = @CurrentReplicaID

      SELECT @CurrentAvailabilityGroupRole = role_desc
      FROM sys.dm_hadr_availability_replica_states
      WHERE replica_id = @CurrentReplicaID

      SELECT @CurrentAvailabilityGroup = [name],
             @CurrentAvailabilityGroupBackupPreference = UPPER(automated_backup_preference_desc)
      FROM sys.availability_groups
      WHERE group_id = @CurrentAvailabilityGroupID
    END

    IF @Version >= 11 AND SERVERPROPERTY('IsHadrEnabled') = 1 AND @CurrentAvailabilityGroup IS NOT NULL
    BEGIN
      SELECT @CurrentIsPreferredBackupReplica = sys.fn_hadr_backup_is_preferred_replica(@CurrentDatabaseName)
    END

    SELECT @CurrentDifferentialBaseLSN = differential_base_lsn
    FROM sys.master_files
    WHERE database_id = DB_ID(@CurrentDatabaseName)
    AND [type] = 0
    AND [file_id] = 1

    IF @CurrentDatabaseState = 'ONLINE' AND NOT (@CurrentInStandby = 1)
    BEGIN
      SELECT @CurrentLogLSN = last_log_backup_lsn
      FROM sys.database_recovery_status
      WHERE database_id = DB_ID(@CurrentDatabaseName)
    END

    IF @CurrentDatabaseState = 'ONLINE' AND NOT (@CurrentInStandby = 1)
    AND EXISTS(SELECT * FROM sys.all_columns WHERE object_id = OBJECT_ID('sys.dm_db_file_space_usage') AND name = 'modified_extent_page_count')
    AND (@CurrentAvailabilityGroupRole = 'PRIMARY' OR @CurrentAvailabilityGroupRole IS NULL)
    AND (@BackupType IN('DIFF','FULL') OR (@ChangeBackupType = 'Y' AND @CurrentBackupType = 'LOG' AND @CurrentRecoveryModel IN('FULL','BULK_LOGGED') AND @CurrentLogLSN IS NULL AND @CurrentDatabaseName <> 'master'))
    AND (@ModificationLevel IS NOT NULL OR @MinBackupSizeForMultipleFiles IS NOT NULL OR @MaxFileSize IS NOT NULL)
    BEGIN
      SET @CurrentCommand = 'SELECT @ParamAllocatedExtentPageCount = SUM(allocated_extent_page_count), @ParamModifiedExtentPageCount = SUM(modified_extent_page_count) FROM sys.dm_db_file_space_usage'

      EXECUTE @CurrentDatabase_sp_executesql @stmt = @CurrentCommand, @params = N'@ParamAllocatedExtentPageCount bigint OUTPUT, @ParamModifiedExtentPageCount bigint OUTPUT', @ParamAllocatedExtentPageCount = @CurrentAllocatedExtentPageCount OUTPUT, @ParamModifiedExtentPageCount = @CurrentModifiedExtentPageCount OUTPUT
    END

    SET @CurrentBackupType = @BackupType

    IF @ChangeBackupType = 'Y'
    BEGIN
      IF @CurrentBackupType = 'LOG' AND @CurrentRecoveryModel IN('FULL','BULK_LOGGED') AND @CurrentLogLSN IS NULL AND @CurrentDatabaseName <> 'master'
      BEGIN
        SET @CurrentBackupType = 'DIFF'
      END
      IF @CurrentBackupType = 'DIFF' AND (@CurrentDatabaseName = 'master' OR @CurrentDifferentialBaseLSN IS NULL OR (@CurrentModifiedExtentPageCount * 1. / @CurrentAllocatedExtentPageCount * 100 >= @ModificationLevel))
      BEGIN
        SET @CurrentBackupType = 'FULL'
      END
    END

    IF @CurrentDatabaseState = 'ONLINE' AND NOT (@CurrentInStandby = 1)
    AND EXISTS(SELECT * FROM sys.all_columns WHERE object_id = OBJECT_ID('sys.dm_db_log_stats') AND name = 'log_since_last_log_backup_mb')
    BEGIN
      SELECT @CurrentLastLogBackup = log_backup_time,
             @CurrentLogSizeSinceLastLogBackup = log_since_last_log_backup_mb
      FROM sys.dm_db_log_stats (DB_ID(@CurrentDatabaseName))
    END

    IF @CurrentBackupType = 'DIFF'
    BEGIN
      SELECT @CurrentDifferentialBaseIsSnapshot = is_snapshot
      FROM msdb.dbo.backupset
      WHERE database_name = @CurrentDatabaseName
      AND [type] = 'D'
      AND checkpoint_lsn = @CurrentDifferentialBaseLSN
    END

    IF @ChangeBackupType = 'Y'
    BEGIN
      IF @CurrentBackupType = 'DIFF' AND @CurrentDifferentialBaseIsSnapshot = 1
      BEGIN
        SET @CurrentBackupType = 'FULL'
      END
    END;

    WITH CurrentDatabase AS
    (
    SELECT BackupSize = CASE WHEN @CurrentBackupType = 'FULL' THEN COALESCE(CAST(@CurrentAllocatedExtentPageCount AS bigint) * 8192, CAST(@CurrentDatabaseSize AS bigint) * 8192)
                             WHEN @CurrentBackupType = 'DIFF' THEN CAST(@CurrentModifiedExtentPageCount AS bigint) * 8192
                             WHEN @CurrentBackupType = 'LOG' THEN CAST(@CurrentLogSizeSinceLastLogBackup * 1024 * 1024 AS bigint)
                             END,
           MaxNumberOfFiles = CASE WHEN @BackupSoftware IN('SQLBACKUP','DATA_DOMAIN_BOOST') THEN 32 ELSE 64 END,
           CASE WHEN (SELECT COUNT(*) FROM @Directories WHERE Mirror = 0) > 0 THEN (SELECT COUNT(*) FROM @Directories WHERE Mirror = 0) ELSE (SELECT COUNT(*) FROM @URLs WHERE Mirror = 0) END AS NumberOfDirectories,
           CAST(@MinBackupSizeForMultipleFiles AS bigint) * 1024 * 1024 AS MinBackupSizeForMultipleFiles,
           CAST(@MaxFileSize AS bigint) * 1024 * 1024 AS MaxFileSize
    )
    SELECT @CurrentNumberOfFiles = CASE WHEN @NumberOfFiles IS NULL AND @BackupSoftware = 'DATA_DOMAIN_BOOST' THEN 1
                                        WHEN @NumberOfFiles IS NULL AND @MaxFileSize IS NULL THEN NumberOfDirectories
                                        WHEN @NumberOfFiles = 1 THEN @NumberOfFiles
                                        WHEN @NumberOfFiles > 1 AND (BackupSize >= MinBackupSizeForMultipleFiles OR MinBackupSizeForMultipleFiles IS NULL OR BackupSize IS NULL) THEN @NumberOfFiles
                                        WHEN @NumberOfFiles > 1 AND (BackupSize < MinBackupSizeForMultipleFiles) THEN NumberOfDirectories
                                        WHEN @NumberOfFiles IS NULL AND @MaxFileSize IS NOT NULL AND (BackupSize IS NULL OR BackupSize = 0) THEN NumberOfDirectories
                                        WHEN @NumberOfFiles IS NULL AND @MaxFileSize IS NOT NULL THEN (SELECT MIN(NumberOfFilesInEachDirectory)
                                                                                                       FROM (SELECT ((BackupSize / NumberOfDirectories) / MaxFileSize + CASE WHEN (BackupSize / NumberOfDirectories) % MaxFileSize = 0 THEN 0 ELSE 1 END) AS NumberOfFilesInEachDirectory
                                                                                                             UNION
                                                                                                             SELECT MaxNumberOfFiles / NumberOfDirectories) Files) * NumberOfDirectories
                                        END

    FROM CurrentDatabase

    SELECT @CurrentDatabaseMirroringRole = UPPER(mirroring_role_desc)
    FROM sys.database_mirroring
    WHERE database_id = DB_ID(@CurrentDatabaseName)

    IF EXISTS (SELECT * FROM msdb.dbo.log_shipping_primary_databases WHERE primary_database = @CurrentDatabaseName)
    BEGIN
      SET @CurrentLogShippingRole = 'PRIMARY'
    END
    ELSE
    IF EXISTS (SELECT * FROM msdb.dbo.log_shipping_secondary_databases WHERE secondary_database = @CurrentDatabaseName)
    BEGIN
      SET @CurrentLogShippingRole = 'SECONDARY'
    END

    IF @CurrentIsDatabaseAccessible IS NOT NULL
    BEGIN
      SET @DatabaseMessage = 'Is accessible: ' + CASE WHEN @CurrentIsDatabaseAccessible = 1 THEN 'Yes' ELSE 'No' END
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
    END

    IF @CurrentAvailabilityGroup IS NOT NULL
    BEGIN
      SET @DatabaseMessage = 'Availability group: ' + ISNULL(@CurrentAvailabilityGroup,'N/A')
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Availability group role: ' + ISNULL(@CurrentAvailabilityGroupRole,'N/A')
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Availability group backup preference: ' + ISNULL(@CurrentAvailabilityGroupBackupPreference,'N/A')
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Is preferred backup replica: ' + CASE WHEN @CurrentIsPreferredBackupReplica = 1 THEN 'Yes' WHEN @CurrentIsPreferredBackupReplica = 0 THEN 'No' ELSE 'N/A' END
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
    END

    IF @CurrentDatabaseMirroringRole IS NOT NULL
    BEGIN
      SET @DatabaseMessage = 'Database mirroring role: ' + @CurrentDatabaseMirroringRole
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
    END

    IF @CurrentLogShippingRole IS NOT NULL
    BEGIN
      SET @DatabaseMessage = 'Log shipping role: ' + @CurrentLogShippingRole
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
    END

    SET @DatabaseMessage = 'Differential base LSN: ' + ISNULL(CAST(@CurrentDifferentialBaseLSN AS nvarchar),'N/A')
    RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

    IF @CurrentBackupType = 'DIFF' OR @CurrentDifferentialBaseIsSnapshot IS NOT NULL
    BEGIN
      SET @DatabaseMessage = 'Differential base is snapshot: ' + CASE WHEN @CurrentDifferentialBaseIsSnapshot = 1 THEN 'Yes' WHEN @CurrentDifferentialBaseIsSnapshot = 0 THEN 'No' ELSE 'N/A' END
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
    END

    SET @DatabaseMessage = 'Last log backup LSN: ' + ISNULL(CAST(@CurrentLogLSN AS nvarchar),'N/A')
    RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

    IF @CurrentBackupType IN('DIFF','FULL') AND EXISTS(SELECT * FROM sys.all_columns WHERE object_id = OBJECT_ID('sys.dm_db_file_space_usage') AND name = 'modified_extent_page_count')
    BEGIN
      SET @DatabaseMessage = 'Allocated extent page count: ' + ISNULL(CAST(@CurrentAllocatedExtentPageCount AS nvarchar) + ' (' + CAST(@CurrentAllocatedExtentPageCount * 1. * 8 / 1024 AS nvarchar) + ' MB)','N/A')
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Modified extent page count: ' + ISNULL(CAST(@CurrentModifiedExtentPageCount AS nvarchar) + ' (' + CAST(@CurrentModifiedExtentPageCount * 1. * 8 / 1024 AS nvarchar) + ' MB)','N/A')
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
    END

    IF @CurrentBackupType = 'LOG' AND EXISTS(SELECT * FROM sys.all_columns WHERE object_id = OBJECT_ID('sys.dm_db_log_stats') AND name = 'log_since_last_log_backup_mb')
    BEGIN
      SET @DatabaseMessage = 'Last log backup: ' + ISNULL(CONVERT(nvarchar(19),NULLIF(@CurrentLastLogBackup,'1900-01-01'),120),'N/A')
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Log size since last log backup (MB): ' + ISNULL(CAST(@CurrentLogSizeSinceLastLogBackup AS nvarchar),'N/A')
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
    END

    RAISERROR(@EmptyLine,10,1) WITH NOWAIT

    IF @CurrentDatabaseState = 'ONLINE'
    AND NOT (@CurrentUserAccess = 'SINGLE_USER' AND @CurrentIsDatabaseAccessible = 0)
    AND NOT (@CurrentInStandby = 1)
    AND NOT (@CurrentBackupType = 'LOG' AND @CurrentRecoveryModel = 'SIMPLE')
    AND NOT (@CurrentBackupType = 'LOG' AND @CurrentRecoveryModel IN('FULL','BULK_LOGGED') AND @CurrentLogLSN IS NULL)
    AND NOT (@CurrentBackupType = 'DIFF' AND @CurrentDifferentialBaseLSN IS NULL)
    AND NOT (@CurrentBackupType IN('DIFF','LOG') AND @CurrentDatabaseName = 'master')
    AND NOT (@CurrentAvailabilityGroup IS NOT NULL AND @CurrentBackupType = 'FULL' AND @CopyOnly = 'N' AND (@CurrentAvailabilityGroupRole <> 'PRIMARY' OR @CurrentAvailabilityGroupRole IS NULL))
    AND NOT (@CurrentAvailabilityGroup IS NOT NULL AND @CurrentBackupType = 'FULL' AND @CopyOnly = 'Y' AND (@CurrentIsPreferredBackupReplica <> 1 OR @CurrentIsPreferredBackupReplica IS NULL) AND @OverrideBackupPreference = 'N')
    AND NOT (@CurrentAvailabilityGroup IS NOT NULL AND @CurrentBackupType = 'DIFF' AND (@CurrentAvailabilityGroupRole <> 'PRIMARY' OR @CurrentAvailabilityGroupRole IS NULL))
    AND NOT (@CurrentAvailabilityGroup IS NOT NULL AND @CurrentBackupType = 'LOG' AND @CopyOnly = 'N' AND (@CurrentIsPreferredBackupReplica <> 1 OR @CurrentIsPreferredBackupReplica IS NULL) AND @OverrideBackupPreference = 'N')
    AND NOT (@CurrentAvailabilityGroup IS NOT NULL AND @CurrentBackupType = 'LOG' AND @CopyOnly = 'Y' AND (@CurrentAvailabilityGroupRole <> 'PRIMARY' OR @CurrentAvailabilityGroupRole IS NULL))
    AND NOT ((@CurrentLogShippingRole = 'PRIMARY' AND @CurrentLogShippingRole IS NOT NULL) AND @CurrentBackupType = 'LOG' AND @ExcludeLogShippedFromLogBackup = 'Y')
    AND NOT (@CurrentIsReadOnly = 1 AND @Updateability = 'READ_WRITE')
    AND NOT (@CurrentIsReadOnly = 0 AND @Updateability = 'READ_ONLY')
    AND NOT (@CurrentBackupType = 'LOG' AND @LogSizeSinceLastLogBackup IS NOT NULL AND @TimeSinceLastLogBackup IS NOT NULL AND NOT(@CurrentLogSizeSinceLastLogBackup >= @LogSizeSinceLastLogBackup OR @CurrentLogSizeSinceLastLogBackup IS NULL OR DATEDIFF(SECOND,@CurrentLastLogBackup,SYSDATETIME()) >= @TimeSinceLastLogBackup OR @CurrentLastLogBackup IS NULL))
    AND NOT (@CurrentBackupType = 'LOG' AND @Updateability = 'READ_ONLY' AND @BackupSoftware = 'DATA_DOMAIN_BOOST')
    BEGIN

      IF @CurrentBackupType = 'LOG' AND (@CleanupTime IS NOT NULL OR @MirrorCleanupTime IS NOT NULL)
      BEGIN
        SELECT @CurrentLatestBackup = MAX(backup_start_date)
        FROM msdb.dbo.backupset
        WHERE ([type] IN('D','I')
        OR ([type] = 'L' AND last_lsn < @CurrentDifferentialBaseLSN))
        AND is_damaged = 0
        AND [database_name] = @CurrentDatabaseName
      END

      SET @CurrentDate = SYSDATETIME()

      INSERT INTO @CurrentCleanupDates (CleanupDate)
      SELECT @CurrentDate

      IF @CurrentBackupType = 'LOG'
      BEGIN
        INSERT INTO @CurrentCleanupDates (CleanupDate)
        SELECT @CurrentLatestBackup
      END

      SELECT @CurrentDirectoryStructure = CASE
      WHEN @CurrentAvailabilityGroup IS NOT NULL THEN @AvailabilityGroupDirectoryStructure
      ELSE @DirectoryStructure
      END

      IF @CurrentDirectoryStructure IS NOT NULL
      BEGIN
      -- Directory structure - remove tokens that are not needed
        IF @ReadWriteFileGroups = 'N' SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{Partial}','')
        IF @CopyOnly = 'N' SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{CopyOnly}','')
        IF @Cluster IS NULL SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{ClusterName}','')
        IF @CurrentAvailabilityGroup IS NULL SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{AvailabilityGroupName}','')
        IF SERVERPROPERTY('InstanceName') IS NULL SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{InstanceName}','')
        IF @@SERVICENAME IS NULL SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{ServiceName}','')
        IF @Description IS NULL SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{Description}','')

        IF @Directory IS NULL AND @MirrorDirectory IS NULL AND @URL IS NULL AND @DefaultDirectory LIKE '%' + '.' + @@SERVICENAME + @DirectorySeparator + 'MSSQL' + @DirectorySeparator + 'Backup'
        BEGIN
          SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{ServerName}','')
          SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{InstanceName}','')
          SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{ClusterName}','')
          SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{AvailabilityGroupName}','')
        END

        WHILE (@Updated = 1 OR @Updated IS NULL)
        BEGIN
          SET @Updated = 0

          IF CHARINDEX('\',@CurrentDirectoryStructure) > 0
          BEGIN
            SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'\','{DirectorySeparator}')
            SET @Updated = 1
          END

          IF CHARINDEX('/',@CurrentDirectoryStructure) > 0
          BEGIN
            SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'/','{DirectorySeparator}')
            SET @Updated = 1
          END

          IF CHARINDEX('__',@CurrentDirectoryStructure) > 0
          BEGIN
            SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'__','_')
            SET @Updated = 1
          END

          IF CHARINDEX('--',@CurrentDirectoryStructure) > 0
          BEGIN
            SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'--','-')
            SET @Updated = 1
          END

          IF CHARINDEX('{DirectorySeparator}{DirectorySeparator}',@CurrentDirectoryStructure) > 0
          BEGIN
            SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{DirectorySeparator}{DirectorySeparator}','{DirectorySeparator}')
            SET @Updated = 1
          END

          IF CHARINDEX('{DirectorySeparator}$',@CurrentDirectoryStructure) > 0
          BEGIN
            SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{DirectorySeparator}$','{DirectorySeparator}')
            SET @Updated = 1
          END
          IF CHARINDEX('${DirectorySeparator}',@CurrentDirectoryStructure) > 0
          BEGIN
            SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'${DirectorySeparator}','{DirectorySeparator}')
            SET @Updated = 1
          END

          IF CHARINDEX('{DirectorySeparator}_',@CurrentDirectoryStructure) > 0
          BEGIN
            SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{DirectorySeparator}_','{DirectorySeparator}')
            SET @Updated = 1
          END
          IF CHARINDEX('_{DirectorySeparator}',@CurrentDirectoryStructure) > 0
          BEGIN
            SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'_{DirectorySeparator}','{DirectorySeparator}')
            SET @Updated = 1
          END

          IF CHARINDEX('{DirectorySeparator}-',@CurrentDirectoryStructure) > 0
          BEGIN
            SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{DirectorySeparator}-','{DirectorySeparator}')
            SET @Updated = 1
          END
          IF CHARINDEX('-{DirectorySeparator}',@CurrentDirectoryStructure) > 0
          BEGIN
            SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'-{DirectorySeparator}','{DirectorySeparator}')
            SET @Updated = 1
          END

          IF CHARINDEX('_$',@CurrentDirectoryStructure) > 0
          BEGIN
            SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'_$','_')
            SET @Updated = 1
          END
          IF CHARINDEX('$_',@CurrentDirectoryStructure) > 0
          BEGIN
            SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'$_','_')
            SET @Updated = 1
          END

          IF CHARINDEX('-$',@CurrentDirectoryStructure) > 0
          BEGIN
            SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'-$','-')
            SET @Updated = 1
          END
          IF CHARINDEX('$-',@CurrentDirectoryStructure) > 0
          BEGIN
            SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'$-','-')
            SET @Updated = 1
          END

          IF LEFT(@CurrentDirectoryStructure,1) = '_'
          BEGIN
            SET @CurrentDirectoryStructure = RIGHT(@CurrentDirectoryStructure,LEN(@CurrentDirectoryStructure) - 1)
            SET @Updated = 1
          END
          IF RIGHT(@CurrentDirectoryStructure,1) = '_'
          BEGIN
            SET @CurrentDirectoryStructure = LEFT(@CurrentDirectoryStructure,LEN(@CurrentDirectoryStructure) - 1)
            SET @Updated = 1
          END

          IF LEFT(@CurrentDirectoryStructure,1) = '-'
          BEGIN
            SET @CurrentDirectoryStructure = RIGHT(@CurrentDirectoryStructure,LEN(@CurrentDirectoryStructure) - 1)
            SET @Updated = 1
          END
          IF RIGHT(@CurrentDirectoryStructure,1) = '-'
          BEGIN
            SET @CurrentDirectoryStructure = LEFT(@CurrentDirectoryStructure,LEN(@CurrentDirectoryStructure) - 1)
            SET @Updated = 1
          END

          IF LEFT(@CurrentDirectoryStructure,1) = '$'
          BEGIN
            SET @CurrentDirectoryStructure = RIGHT(@CurrentDirectoryStructure,LEN(@CurrentDirectoryStructure) - 1)
            SET @Updated = 1
          END
          IF RIGHT(@CurrentDirectoryStructure,1) = '$'
          BEGIN
            SET @CurrentDirectoryStructure = LEFT(@CurrentDirectoryStructure,LEN(@CurrentDirectoryStructure) - 1)
            SET @Updated = 1
          END

          IF LEFT(@CurrentDirectoryStructure,20) = '{DirectorySeparator}'
          BEGIN
            SET @CurrentDirectoryStructure = RIGHT(@CurrentDirectoryStructure,LEN(@CurrentDirectoryStructure) - 20)
            SET @Updated = 1
          END
          IF RIGHT(@CurrentDirectoryStructure,20) = '{DirectorySeparator}'
          BEGIN
            SET @CurrentDirectoryStructure = LEFT(@CurrentDirectoryStructure,LEN(@CurrentDirectoryStructure) - 20)
            SET @Updated = 1
          END
        END

        SET @Updated = NULL

        -- Directory structure - replace tokens with real values
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{DirectorySeparator}',@DirectorySeparator)
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{ServerName}',CASE WHEN SERVERPROPERTY('EngineEdition') = 8 THEN LEFT(CAST(SERVERPROPERTY('ServerName') AS nvarchar(max)),CHARINDEX('.',CAST(SERVERPROPERTY('ServerName') AS nvarchar(max))) - 1) ELSE CAST(SERVERPROPERTY('MachineName') AS nvarchar(max)) END)
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{InstanceName}',ISNULL(CAST(SERVERPROPERTY('InstanceName') AS nvarchar(max)),''))
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{ServiceName}',ISNULL(@@SERVICENAME,''))
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{ClusterName}',ISNULL(@Cluster,''))
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{AvailabilityGroupName}',ISNULL(@CurrentAvailabilityGroup,''))
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{DatabaseName}',@CurrentDatabaseNameFS)
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{BackupType}',@CurrentBackupType)
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{Partial}','PARTIAL')
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{CopyOnly}','COPY_ONLY')
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{Description}',LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(ISNULL(@Description,''),'\',''),'/',''),':',''),'*',''),'?',''),'"',''),'<',''),'>',''),'|',''))))
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{Year}',CAST(DATEPART(YEAR,@CurrentDate) AS nvarchar))
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{Month}',RIGHT('0' + CAST(DATEPART(MONTH,@CurrentDate) AS nvarchar),2))
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{Day}',RIGHT('0' + CAST(DATEPART(DAY,@CurrentDate) AS nvarchar),2))
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{Week}',RIGHT('0' + CAST(DATEPART(WEEK,@CurrentDate) AS nvarchar),2))
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{Hour}',RIGHT('0' + CAST(DATEPART(HOUR,@CurrentDate) AS nvarchar),2))
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{Minute}',RIGHT('0' + CAST(DATEPART(MINUTE,@CurrentDate) AS nvarchar),2))
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{Second}',RIGHT('0' + CAST(DATEPART(SECOND,@CurrentDate) AS nvarchar),2))
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{Millisecond}',RIGHT('00' + CAST(DATEPART(MILLISECOND,@CurrentDate) AS nvarchar),3))
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{Microsecond}',RIGHT('00000' + CAST(DATEPART(MICROSECOND,@CurrentDate) AS nvarchar),6))
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{MajorVersion}',ISNULL(CAST(SERVERPROPERTY('ProductMajorVersion') AS nvarchar),PARSENAME(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar),4)))
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{MinorVersion}',ISNULL(CAST(SERVERPROPERTY('ProductMinorVersion') AS nvarchar),PARSENAME(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar),3)))
      END

      INSERT INTO @CurrentDirectories (ID, DirectoryPath, Mirror, DirectoryNumber, CreateCompleted, CleanupCompleted)
      SELECT ROW_NUMBER() OVER (ORDER BY ID),
             DirectoryPath + CASE WHEN DirectoryPath = 'NUL' THEN '' WHEN @CurrentDirectoryStructure IS NOT NULL THEN @DirectorySeparator + @CurrentDirectoryStructure ELSE '' END,
             Mirror,
             ROW_NUMBER() OVER (PARTITION BY Mirror ORDER BY ID ASC),
             0,
             0
      FROM @Directories
      ORDER BY ID ASC

      INSERT INTO @CurrentURLs (ID, DirectoryPath, Mirror, DirectoryNumber)
      SELECT ROW_NUMBER() OVER (ORDER BY ID),
             DirectoryPath + CASE WHEN @CurrentDirectoryStructure IS NOT NULL THEN @DirectorySeparator + @CurrentDirectoryStructure ELSE '' END,
             Mirror,
             ROW_NUMBER() OVER (PARTITION BY Mirror ORDER BY ID ASC)
      FROM @URLs
      ORDER BY ID ASC

      SELECT @CurrentDatabaseFileName = CASE
      WHEN @CurrentAvailabilityGroup IS NOT NULL THEN @AvailabilityGroupFileName
      ELSE @FileName
      END

      -- File name - remove tokens that are not needed
      IF @ReadWriteFileGroups = 'N' SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{Partial}','')
      IF @CopyOnly = 'N' SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{CopyOnly}','')
      IF @Cluster IS NULL SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{ClusterName}','')
      IF @CurrentAvailabilityGroup IS NULL SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{AvailabilityGroupName}','')
      IF SERVERPROPERTY('InstanceName') IS NULL SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{InstanceName}','')
      IF @@SERVICENAME IS NULL SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{ServiceName}','')
      IF @Description IS NULL SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{Description}','')
      IF @CurrentNumberOfFiles = 1 SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{FileNumber}','')
      IF @CurrentNumberOfFiles = 1 SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{NumberOfFiles}','')

      WHILE (@Updated = 1 OR @Updated IS NULL)
      BEGIN
        SET @Updated = 0

        IF CHARINDEX('__',@CurrentDatabaseFileName) > 0
        BEGIN
          SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'__','_')
          SET @Updated = 1
        END

        IF CHARINDEX('--',@CurrentDatabaseFileName) > 0
        BEGIN
          SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'--','-')
          SET @Updated = 1
        END

        IF CHARINDEX('_$',@CurrentDatabaseFileName) > 0
        BEGIN
          SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'_$','_')
          SET @Updated = 1
        END
        IF CHARINDEX('$_',@CurrentDatabaseFileName) > 0
        BEGIN
          SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'$_','_')
          SET @Updated = 1
        END

        IF CHARINDEX('-$',@CurrentDatabaseFileName) > 0
        BEGIN
          SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'-$','-')
          SET @Updated = 1
        END
        IF CHARINDEX('$-',@CurrentDatabaseFileName) > 0
        BEGIN
          SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'$-','-')
          SET @Updated = 1
        END

        IF CHARINDEX('_.',@CurrentDatabaseFileName) > 0
        BEGIN
          SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'_.','.')
          SET @Updated = 1
        END

        IF CHARINDEX('-.',@CurrentDatabaseFileName) > 0
        BEGIN
          SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'-.','.')
          SET @Updated = 1
        END

        IF CHARINDEX('of.',@CurrentDatabaseFileName) > 0
        BEGIN
          SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'of.','.')
          SET @Updated = 1
        END

        IF LEFT(@CurrentDatabaseFileName,1) = '_'
        BEGIN
          SET @CurrentDatabaseFileName = RIGHT(@CurrentDatabaseFileName,LEN(@CurrentDatabaseFileName) - 1)
          SET @Updated = 1
        END
        IF RIGHT(@CurrentDatabaseFileName,1) = '_'
        BEGIN
          SET @CurrentDatabaseFileName = LEFT(@CurrentDatabaseFileName,LEN(@CurrentDatabaseFileName) - 1)
          SET @Updated = 1
        END

        IF LEFT(@CurrentDatabaseFileName,1) = '-'
        BEGIN
          SET @CurrentDatabaseFileName = RIGHT(@CurrentDatabaseFileName,LEN(@CurrentDatabaseFileName) - 1)
          SET @Updated = 1
        END
        IF RIGHT(@CurrentDatabaseFileName,1) = '-'
        BEGIN
          SET @CurrentDatabaseFileName = LEFT(@CurrentDatabaseFileName,LEN(@CurrentDatabaseFileName) - 1)
          SET @Updated = 1
        END

        IF LEFT(@CurrentDatabaseFileName,1) = '$'
        BEGIN
          SET @CurrentDatabaseFileName = RIGHT(@CurrentDatabaseFileName,LEN(@CurrentDatabaseFileName) - 1)
          SET @Updated = 1
        END
        IF RIGHT(@CurrentDatabaseFileName,1) = '$'
        BEGIN
          SET @CurrentDatabaseFileName = LEFT(@CurrentDatabaseFileName,LEN(@CurrentDatabaseFileName) - 1)
          SET @Updated = 1
        END
      END

      SET @Updated = NULL

      SELECT @CurrentFileExtension = CASE
      WHEN @CurrentBackupType = 'FULL' THEN @FileExtensionFull
      WHEN @CurrentBackupType = 'DIFF' THEN @FileExtensionDiff
      WHEN @CurrentBackupType = 'LOG' THEN @FileExtensionLog
      END

      -- File name - replace tokens with real values
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{ServerName}',CASE WHEN SERVERPROPERTY('EngineEdition') = 8 THEN LEFT(CAST(SERVERPROPERTY('ServerName') AS nvarchar(max)),CHARINDEX('.',CAST(SERVERPROPERTY('ServerName') AS nvarchar(max))) - 1) ELSE CAST(SERVERPROPERTY('MachineName') AS nvarchar(max)) END)
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{InstanceName}',ISNULL(CAST(SERVERPROPERTY('InstanceName') AS nvarchar(max)),''))
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{ServiceName}',ISNULL(@@SERVICENAME,''))
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{ClusterName}',ISNULL(@Cluster,''))
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{AvailabilityGroupName}',ISNULL(@CurrentAvailabilityGroup,''))
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{BackupType}',@CurrentBackupType)
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{Partial}','PARTIAL')
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{CopyOnly}','COPY_ONLY')
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{Description}',LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(ISNULL(@Description,''),'\',''),'/',''),':',''),'*',''),'?',''),'"',''),'<',''),'>',''),'|',''))))
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{Year}',CAST(DATEPART(YEAR,@CurrentDate) AS nvarchar))
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{Month}',RIGHT('0' + CAST(DATEPART(MONTH,@CurrentDate) AS nvarchar),2))
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{Day}',RIGHT('0' + CAST(DATEPART(DAY,@CurrentDate) AS nvarchar),2))
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{Week}',RIGHT('0' + CAST(DATEPART(WEEK,@CurrentDate) AS nvarchar),2))
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{Hour}',RIGHT('0' + CAST(DATEPART(HOUR,@CurrentDate) AS nvarchar),2))
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{Minute}',RIGHT('0' + CAST(DATEPART(MINUTE,@CurrentDate) AS nvarchar),2))
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{Second}',RIGHT('0' + CAST(DATEPART(SECOND,@CurrentDate) AS nvarchar),2))
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{Millisecond}',RIGHT('00' + CAST(DATEPART(MILLISECOND,@CurrentDate) AS nvarchar),3))
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{Microsecond}',RIGHT('00000' + CAST(DATEPART(MICROSECOND,@CurrentDate) AS nvarchar),6))
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{NumberOfFiles}',@CurrentNumberOfFiles)
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{FileExtension}',@CurrentFileExtension)
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{MajorVersion}',ISNULL(CAST(SERVERPROPERTY('ProductMajorVersion') AS nvarchar),PARSENAME(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar),4)))
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{MinorVersion}',ISNULL(CAST(SERVERPROPERTY('ProductMinorVersion') AS nvarchar),PARSENAME(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar),3)))

      SELECT @CurrentMaxFilePathLength = CASE
      WHEN EXISTS (SELECT * FROM @CurrentDirectories) THEN (SELECT MAX(LEN(DirectoryPath + @DirectorySeparator)) FROM @CurrentDirectories)
      WHEN EXISTS (SELECT * FROM @CurrentURLs) THEN (SELECT MAX(LEN(DirectoryPath + @DirectorySeparator)) FROM @CurrentURLs)
      END
      + LEN(REPLACE(REPLACE(@CurrentDatabaseFileName,'{DatabaseName}',@CurrentDatabaseNameFS), '{FileNumber}', CASE WHEN @CurrentNumberOfFiles >= 1 AND @CurrentNumberOfFiles <= 9 THEN '1' WHEN @CurrentNumberOfFiles >= 10 THEN '01' END))

      -- The maximum length of a backup device is 259 characters
      IF @CurrentMaxFilePathLength > 259
      BEGIN
        SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{DatabaseName}',LEFT(@CurrentDatabaseNameFS,CASE WHEN (LEN(@CurrentDatabaseNameFS) + 259 - @CurrentMaxFilePathLength - 3) < 20 THEN 20 ELSE (LEN(@CurrentDatabaseNameFS) + 259 - @CurrentMaxFilePathLength - 3) END) + '...')
      END
      ELSE
      BEGIN
        SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{DatabaseName}',@CurrentDatabaseNameFS)
      END

      IF EXISTS (SELECT * FROM @CurrentDirectories WHERE Mirror = 0)
      BEGIN
        SET @CurrentFileNumber = 0

        WHILE @CurrentFileNumber < @CurrentNumberOfFiles
        BEGIN
          SET @CurrentFileNumber = @CurrentFileNumber + 1

          SELECT @CurrentDirectoryPath = DirectoryPath
          FROM @CurrentDirectories
          WHERE @CurrentFileNumber >= (DirectoryNumber - 1) * (SELECT @CurrentNumberOfFiles / COUNT(*) FROM @CurrentDirectories WHERE Mirror = 0) + 1
          AND @CurrentFileNumber <= DirectoryNumber * (SELECT @CurrentNumberOfFiles / COUNT(*) FROM @CurrentDirectories WHERE Mirror = 0)
          AND Mirror = 0

          SET @CurrentFileName = REPLACE(@CurrentDatabaseFileName, '{FileNumber}', CASE WHEN @CurrentNumberOfFiles >= 1 AND @CurrentNumberOfFiles <= 9 THEN CAST(@CurrentFileNumber AS nvarchar) WHEN @CurrentNumberOfFiles >= 10 THEN RIGHT('0' + CAST(@CurrentFileNumber AS nvarchar),2) END)

          IF @CurrentDirectoryPath = 'NUL'
          BEGIN
            SET @CurrentFilePath = 'NUL'
          END
          ELSE
          BEGIN
            SET @CurrentFilePath = @CurrentDirectoryPath + @DirectorySeparator + @CurrentFileName
          END

          INSERT INTO @CurrentFiles ([Type], FilePath, Mirror)
          SELECT 'DISK', @CurrentFilePath, 0

          SET @CurrentDirectoryPath = NULL
          SET @CurrentFileName = NULL
          SET @CurrentFilePath = NULL
        END

        INSERT INTO @CurrentBackupSet (Mirror, VerifyCompleted)
        SELECT 0, 0
      END

      IF EXISTS (SELECT * FROM @CurrentDirectories WHERE Mirror = 1)
      BEGIN
        SET @CurrentFileNumber = 0

        WHILE @CurrentFileNumber < @CurrentNumberOfFiles
        BEGIN
          SET @CurrentFileNumber = @CurrentFileNumber + 1

          SELECT @CurrentDirectoryPath = DirectoryPath
          FROM @CurrentDirectories
          WHERE @CurrentFileNumber >= (DirectoryNumber - 1) * (SELECT @CurrentNumberOfFiles / COUNT(*) FROM @CurrentDirectories WHERE Mirror = 1) + 1
          AND @CurrentFileNumber <= DirectoryNumber * (SELECT @CurrentNumberOfFiles / COUNT(*) FROM @CurrentDirectories WHERE Mirror = 1)
          AND Mirror = 1

          SET @CurrentFileName = REPLACE(@CurrentDatabaseFileName, '{FileNumber}', CASE WHEN @CurrentNumberOfFiles > 1 AND @CurrentNumberOfFiles <= 9 THEN CAST(@CurrentFileNumber AS nvarchar) WHEN @CurrentNumberOfFiles >= 10 THEN RIGHT('0' + CAST(@CurrentFileNumber AS nvarchar),2) ELSE '' END)

          SET @CurrentFilePath = @CurrentDirectoryPath + @DirectorySeparator + @CurrentFileName

          INSERT INTO @CurrentFiles ([Type], FilePath, Mirror)
          SELECT 'DISK', @CurrentFilePath, 1

          SET @CurrentDirectoryPath = NULL
          SET @CurrentFileName = NULL
          SET @CurrentFilePath = NULL
        END

        INSERT INTO @CurrentBackupSet (Mirror, VerifyCompleted)
        SELECT 1, 0
      END

      IF EXISTS (SELECT * FROM @CurrentURLs WHERE Mirror = 0)
      BEGIN
        SET @CurrentFileNumber = 0

        WHILE @CurrentFileNumber < @CurrentNumberOfFiles
        BEGIN
          SET @CurrentFileNumber = @CurrentFileNumber + 1

          SELECT @CurrentDirectoryPath = DirectoryPath
          FROM @CurrentURLs
          WHERE @CurrentFileNumber >= (DirectoryNumber - 1) * (SELECT @CurrentNumberOfFiles / COUNT(*) FROM @CurrentURLs WHERE Mirror = 0) + 1
          AND @CurrentFileNumber <= DirectoryNumber * (SELECT @CurrentNumberOfFiles / COUNT(*) FROM @CurrentURLs WHERE Mirror = 0)
          AND Mirror = 0

          SET @CurrentFileName = REPLACE(@CurrentDatabaseFileName, '{FileNumber}', CASE WHEN @CurrentNumberOfFiles > 1 AND @CurrentNumberOfFiles <= 9 THEN CAST(@CurrentFileNumber AS nvarchar) WHEN @CurrentNumberOfFiles >= 10 THEN RIGHT('0' + CAST(@CurrentFileNumber AS nvarchar),2) ELSE '' END)

          SET @CurrentFilePath = @CurrentDirectoryPath + @DirectorySeparator + @CurrentFileName

          INSERT INTO @CurrentFiles ([Type], FilePath, Mirror)
          SELECT 'URL', @CurrentFilePath, 0

          SET @CurrentDirectoryPath = NULL
          SET @CurrentFileName = NULL
          SET @CurrentFilePath = NULL
        END

        INSERT INTO @CurrentBackupSet (Mirror, VerifyCompleted)
        SELECT 0, 0
      END

      IF EXISTS (SELECT * FROM @CurrentURLs WHERE Mirror = 1)
      BEGIN
        SET @CurrentFileNumber = 0

        WHILE @CurrentFileNumber < @CurrentNumberOfFiles
        BEGIN
          SET @CurrentFileNumber = @CurrentFileNumber + 1

          SELECT @CurrentDirectoryPath = DirectoryPath
          FROM @CurrentURLs
          WHERE @CurrentFileNumber >= (DirectoryNumber - 1) * (SELECT @CurrentNumberOfFiles / COUNT(*) FROM @CurrentURLs WHERE Mirror = 0) + 1
          AND @CurrentFileNumber <= DirectoryNumber * (SELECT @CurrentNumberOfFiles / COUNT(*) FROM @CurrentURLs WHERE Mirror = 0)
          AND Mirror = 1

          SET @CurrentFileName = REPLACE(@CurrentDatabaseFileName, '{FileNumber}', CASE WHEN @CurrentNumberOfFiles > 1 AND @CurrentNumberOfFiles <= 9 THEN CAST(@CurrentFileNumber AS nvarchar) WHEN @CurrentNumberOfFiles >= 10 THEN RIGHT('0' + CAST(@CurrentFileNumber AS nvarchar),2) ELSE '' END)

          SET @CurrentFilePath = @CurrentDirectoryPath + @DirectorySeparator + @CurrentFileName

          INSERT INTO @CurrentFiles ([Type], FilePath, Mirror)
          SELECT 'URL', @CurrentFilePath, 1

          SET @CurrentDirectoryPath = NULL
          SET @CurrentFileName = NULL
          SET @CurrentFilePath = NULL
        END

        INSERT INTO @CurrentBackupSet (Mirror, VerifyCompleted)
        SELECT 1, 0
      END

      -- Create directory
      IF @HostPlatform = 'Windows'
      AND (@BackupSoftware <> 'DATA_DOMAIN_BOOST' OR @BackupSoftware IS NULL)
      AND NOT EXISTS(SELECT * FROM @CurrentDirectories WHERE DirectoryPath = 'NUL' OR DirectoryPath IN(SELECT DirectoryPath FROM @Directories))
      BEGIN
        WHILE (1 = 1)
        BEGIN
          SELECT TOP 1 @CurrentDirectoryID = ID,
                       @CurrentDirectoryPath = DirectoryPath
          FROM @CurrentDirectories
          WHERE CreateCompleted = 0
          ORDER BY ID ASC

          IF @@ROWCOUNT = 0
          BEGIN
            BREAK
          END

          SET @CurrentDatabaseContext = 'master'

          SET @CurrentCommandType = 'xp_create_subdir'

          SET @CurrentCommand = 'DECLARE @ReturnCode int EXECUTE @ReturnCode = dbo.xp_create_subdir N''' + REPLACE(@CurrentDirectoryPath,'''','''''') + ''' IF @ReturnCode <> 0 RAISERROR(''Error creating directory.'', 16, 1)'

          EXECUTE @CurrentCommandOutput = dbo.CommandExecute @DatabaseContext = @CurrentDatabaseContext, @Command = @CurrentCommand, @CommandType = @CurrentCommandType, @Mode = 1, @DatabaseName = @CurrentDatabaseName, @LogToTable = @LogToTable, @Execute = @Execute
          SET @Error = @@ERROR
          IF @Error <> 0 SET @CurrentCommandOutput = @Error
          IF @CurrentCommandOutput <> 0 SET @ReturnCode = @CurrentCommandOutput

          UPDATE @CurrentDirectories
          SET CreateCompleted = 1,
              CreateOutput = @CurrentCommandOutput
          WHERE ID = @CurrentDirectoryID

          SET @CurrentDirectoryID = NULL
          SET @CurrentDirectoryPath = NULL

          SET @CurrentDatabaseContext = NULL
          SET @CurrentCommand = NULL
          SET @CurrentCommandOutput = NULL
          SET @CurrentCommandType = NULL
        END
      END

      IF @CleanupMode = 'BEFORE_BACKUP'
      BEGIN
        INSERT INTO @CurrentCleanupDates (CleanupDate, Mirror)
        SELECT DATEADD(hh,-(@CleanupTime),SYSDATETIME()), 0

        IF NOT EXISTS(SELECT * FROM @CurrentCleanupDates WHERE (Mirror = 0 OR Mirror IS NULL) AND CleanupDate IS NULL)
        BEGIN
          UPDATE @CurrentDirectories
          SET CleanupDate = (SELECT MIN(CleanupDate)
                             FROM @CurrentCleanupDates
                             WHERE (Mirror = 0 OR Mirror IS NULL)),
              CleanupMode = 'BEFORE_BACKUP'
          WHERE Mirror = 0
        END
      END

      IF @MirrorCleanupMode = 'BEFORE_BACKUP'
      BEGIN
        INSERT INTO @CurrentCleanupDates (CleanupDate, Mirror)
        SELECT DATEADD(hh,-(@MirrorCleanupTime),SYSDATETIME()), 1

        IF NOT EXISTS(SELECT * FROM @CurrentCleanupDates WHERE (Mirror = 1 OR Mirror IS NULL) AND CleanupDate IS NULL)
        BEGIN
          UPDATE @CurrentDirectories
          SET CleanupDate = (SELECT MIN(CleanupDate)
                             FROM @CurrentCleanupDates
                             WHERE (Mirror = 1 OR Mirror IS NULL)),
              CleanupMode = 'BEFORE_BACKUP'
          WHERE Mirror = 1
        END
      END

      -- Delete old backup files, before backup
      IF NOT EXISTS (SELECT * FROM @CurrentDirectories WHERE CreateOutput <> 0 OR CreateOutput IS NULL)
      AND (@BackupSoftware <> 'DATA_DOMAIN_BOOST' OR @BackupSoftware IS NULL)
      AND @CurrentBackupType = @BackupType
      BEGIN
        WHILE (1 = 1)
        BEGIN
          SELECT TOP 1 @CurrentDirectoryID = ID,
                       @CurrentDirectoryPath = DirectoryPath,
                       @CurrentCleanupDate = CleanupDate
          FROM @CurrentDirectories
          WHERE CleanupDate IS NOT NULL
          AND CleanupMode = 'BEFORE_BACKUP'
          AND CleanupCompleted = 0
          ORDER BY ID ASC

          IF @@ROWCOUNT = 0
          BEGIN
            BREAK
          END

          IF @BackupSoftware IS NULL
          BEGIN
            SET @CurrentDatabaseContext = 'master'

            SET @CurrentCommandType = 'xp_delete_file'

            SET @CurrentCommand = 'DECLARE @ReturnCode int EXECUTE @ReturnCode = dbo.xp_delete_file 0, N''' + REPLACE(@CurrentDirectoryPath,'''','''''') + ''', ''' + @CurrentFileExtension + ''', ''' + CONVERT(nvarchar(19),@CurrentCleanupDate,126) + ''' IF @ReturnCode <> 0 RAISERROR(''Error deleting files.'', 16, 1)'
          END

          IF @BackupSoftware = 'LITESPEED'
          BEGIN
            SET @CurrentDatabaseContext = 'master'

            SET @CurrentCommandType = 'xp_slssqlmaint'

            SET @CurrentCommand = 'DECLARE @ReturnCode int EXECUTE @ReturnCode = dbo.xp_slssqlmaint N''-MAINTDEL -DELFOLDER "' + REPLACE(@CurrentDirectoryPath,'''','''''') + '" -DELEXTENSION "' + @CurrentFileExtension + '" -DELUNIT "' + CAST(DATEDIFF(mi,@CurrentCleanupDate,SYSDATETIME()) + 1 AS nvarchar) + '" -DELUNITTYPE "minutes" -DELUSEAGE'' IF @ReturnCode <> 0 RAISERROR(''Error deleting LiteSpeed backup files.'', 16, 1)'
          END

          IF @BackupSoftware = 'SQLBACKUP'
          BEGIN
            SET @CurrentDatabaseContext = 'master'

            SET @CurrentCommandType = 'sqbutility'

            SET @CurrentCommand = 'DECLARE @ReturnCode int EXECUTE @ReturnCode = dbo.sqbutility 1032, N''' + REPLACE(@CurrentDatabaseName,'''','''''') + ''', N''' + REPLACE(@CurrentDirectoryPath,'''','''''') + ''', ''' + CASE WHEN @CurrentBackupType = 'FULL' THEN 'D' WHEN @CurrentBackupType = 'DIFF' THEN 'I' WHEN @CurrentBackupType = 'LOG' THEN 'L' END + ''', ''' + CAST(DATEDIFF(hh,@CurrentCleanupDate,SYSDATETIME()) + 1 AS nvarchar) + 'h'', ' + ISNULL('''' + REPLACE(@EncryptionKey,'''','''''') + '''','NULL') + ' IF @ReturnCode <> 0 RAISERROR(''Error deleting SQLBackup backup files.'', 16, 1)'
          END

          IF @BackupSoftware = 'SQLSAFE'
          BEGIN
            SET @CurrentDatabaseContext = 'master'

            SET @CurrentCommandType = 'xp_ss_delete'

            SET @CurrentCommand = 'DECLARE @ReturnCode int EXECUTE @ReturnCode = dbo.xp_ss_delete @filename = N''' + REPLACE(@CurrentDirectoryPath,'''','''''') + '\*.' + @CurrentFileExtension + ''', @age = ''' + CAST(DATEDIFF(mi,@CurrentCleanupDate,SYSDATETIME()) + 1 AS nvarchar) + 'Minutes'' IF @ReturnCode <> 0 RAISERROR(''Error deleting SQLsafe backup files.'', 16, 1)'
          END

          EXECUTE @CurrentCommandOutput = dbo.CommandExecute @DatabaseContext = @CurrentDatabaseContext, @Command = @CurrentCommand, @CommandType = @CurrentCommandType, @Mode = 1, @DatabaseName = @CurrentDatabaseName, @LogToTable = @LogToTable, @Execute = @Execute
          SET @Error = @@ERROR
          IF @Error <> 0 SET @CurrentCommandOutput = @Error
          IF @CurrentCommandOutput <> 0 SET @ReturnCode = @CurrentCommandOutput

          UPDATE @CurrentDirectories
          SET CleanupCompleted = 1,
              CleanupOutput = @CurrentCommandOutput
          WHERE ID = @CurrentDirectoryID

          SET @CurrentDirectoryID = NULL
          SET @CurrentDirectoryPath = NULL
          SET @CurrentCleanupDate = NULL

          SET @CurrentDatabaseContext = NULL
          SET @CurrentCommand = NULL
          SET @CurrentCommandOutput = NULL
          SET @CurrentCommandType = NULL
        END
      END

      -- Perform a backup
      IF NOT EXISTS (SELECT * FROM @CurrentDirectories WHERE DirectoryPath <> 'NUL' AND DirectoryPath NOT IN(SELECT DirectoryPath FROM @Directories) AND (CreateOutput <> 0 OR CreateOutput IS NULL))
      OR @HostPlatform = 'Linux'
      BEGIN
        IF @BackupSoftware IS NULL
        BEGIN
          SET @CurrentDatabaseContext = 'master'

          SELECT @CurrentCommandType = CASE
          WHEN @CurrentBackupType IN('DIFF','FULL') THEN 'BACKUP_DATABASE'
          WHEN @CurrentBackupType = 'LOG' THEN 'BACKUP_LOG'
          END

          SELECT @CurrentCommand = CASE
          WHEN @CurrentBackupType IN('DIFF','FULL') THEN 'BACKUP DATABASE ' + QUOTENAME(@CurrentDatabaseName)
          WHEN @CurrentBackupType = 'LOG' THEN 'BACKUP LOG ' + QUOTENAME(@CurrentDatabaseName)
          END

          IF @ReadWriteFileGroups = 'Y' AND @CurrentDatabaseName <> 'master' SET @CurrentCommand += ' READ_WRITE_FILEGROUPS'

          SET @CurrentCommand += ' TO'

          SELECT @CurrentCommand += ' ' + [Type] + ' = N''' + REPLACE(FilePath,'''','''''') + '''' + CASE WHEN ROW_NUMBER() OVER (ORDER BY FilePath ASC) <> @CurrentNumberOfFiles THEN ',' ELSE '' END
          FROM @CurrentFiles
          WHERE Mirror = 0
          ORDER BY FilePath ASC

          IF EXISTS(SELECT * FROM @CurrentFiles WHERE Mirror = 1)
          BEGIN
            SET @CurrentCommand += ' MIRROR TO'

            SELECT @CurrentCommand += ' ' + [Type] + ' = N''' + REPLACE(FilePath,'''','''''') + '''' + CASE WHEN ROW_NUMBER() OVER (ORDER BY FilePath ASC) <> @CurrentNumberOfFiles THEN ',' ELSE '' END
            FROM @CurrentFiles
            WHERE Mirror = 1
            ORDER BY FilePath ASC
          END

          SET @CurrentCommand += ' WITH '
          IF @CheckSum = 'Y' SET @CurrentCommand += 'CHECKSUM'
          IF @CheckSum = 'N' SET @CurrentCommand += 'NO_CHECKSUM'

          IF @Version >= 10
          BEGIN
            SET @CurrentCommand += CASE WHEN @Compress = 'Y' AND (@CurrentIsEncrypted = 0 OR (@CurrentIsEncrypted = 1 AND ((@Version >= 13 AND @CurrentMaxTransferSize >= 65537) OR @Version >= 15.0404316 OR SERVERPROPERTY('EngineEdition') = 8))) THEN ', COMPRESSION' ELSE ', NO_COMPRESSION' END
          END

          IF @CurrentBackupType = 'DIFF' SET @CurrentCommand += ', DIFFERENTIAL'

          IF EXISTS(SELECT * FROM @CurrentFiles WHERE Mirror = 1)
          BEGIN
            SET @CurrentCommand += ', FORMAT'
          END

          IF @CopyOnly = 'Y' SET @CurrentCommand += ', COPY_ONLY'
          IF @NoRecovery = 'Y' AND @CurrentBackupType = 'LOG' SET @CurrentCommand += ', NORECOVERY'
          IF @Init = 'Y' SET @CurrentCommand += ', INIT'
          IF @Format = 'Y' SET @CurrentCommand += ', FORMAT'
          IF @BlockSize IS NOT NULL SET @CurrentCommand += ', BLOCKSIZE = ' + CAST(@BlockSize AS nvarchar)
          IF @BufferCount IS NOT NULL SET @CurrentCommand += ', BUFFERCOUNT = ' + CAST(@BufferCount AS nvarchar)
          IF @CurrentMaxTransferSize IS NOT NULL SET @CurrentCommand += ', MAXTRANSFERSIZE = ' + CAST(@CurrentMaxTransferSize AS nvarchar)
          IF @Description IS NOT NULL SET @CurrentCommand += ', DESCRIPTION = N''' + REPLACE(@Description,'''','''''') + ''''
          IF @Encrypt = 'Y' SET @CurrentCommand += ', ENCRYPTION (ALGORITHM = ' + UPPER(@EncryptionAlgorithm) + ', '
          IF @Encrypt = 'Y' AND @ServerCertificate IS NOT NULL SET @CurrentCommand += 'SERVER CERTIFICATE = ' + QUOTENAME(@ServerCertificate)
          IF @Encrypt = 'Y' AND @ServerAsymmetricKey IS NOT NULL SET @CurrentCommand += 'SERVER ASYMMETRIC KEY = ' + QUOTENAME(@ServerAsymmetricKey)
          IF @Encrypt = 'Y' SET @CurrentCommand += ')'
          IF @URL IS NOT NULL AND @Credential IS NOT NULL SET @CurrentCommand += ', CREDENTIAL = N''' + REPLACE(@Credential,'''','''''') + ''''
        END

        IF @BackupSoftware = 'LITESPEED'
        BEGIN
          SET @CurrentDatabaseContext = 'master'

          SELECT @CurrentCommandType = CASE
          WHEN @CurrentBackupType IN('DIFF','FULL') THEN 'xp_backup_database'
          WHEN @CurrentBackupType = 'LOG' THEN 'xp_backup_log'
          END

          SELECT @CurrentCommand = CASE
          WHEN @CurrentBackupType IN('DIFF','FULL') THEN 'DECLARE @ReturnCode int EXECUTE @ReturnCode = dbo.xp_backup_database @database = N''' + REPLACE(@CurrentDatabaseName,'''','''''') + ''''
          WHEN @CurrentBackupType = 'LOG' THEN 'DECLARE @ReturnCode int EXECUTE @ReturnCode = dbo.xp_backup_log @database = N''' + REPLACE(@CurrentDatabaseName,'''','''''') + ''''
          END

          SELECT @CurrentCommand += ', @filename = N''' + REPLACE(FilePath,'''','''''') + ''''
          FROM @CurrentFiles
          WHERE Mirror = 0
          ORDER BY FilePath ASC

          IF EXISTS(SELECT * FROM @CurrentFiles WHERE Mirror = 1)
          BEGIN
            SELECT @CurrentCommand += ', @mirror = N''' + REPLACE(FilePath,'''','''''') + ''''
            FROM @CurrentFiles
            WHERE Mirror = 1
            ORDER BY FilePath ASC
          END

          SET @CurrentCommand += ', @with = '''
          IF @CheckSum = 'Y' SET @CurrentCommand += 'CHECKSUM'
          IF @CheckSum = 'N' SET @CurrentCommand += 'NO_CHECKSUM'
          IF @CurrentBackupType = 'DIFF' SET @CurrentCommand += ', DIFFERENTIAL'
          IF @CopyOnly = 'Y' SET @CurrentCommand += ', COPY_ONLY'
          IF @NoRecovery = 'Y' AND @CurrentBackupType = 'LOG' SET @CurrentCommand += ', NORECOVERY'
          IF @BlockSize IS NOT NULL SET @CurrentCommand += ', BLOCKSIZE = ' + CAST(@BlockSize AS nvarchar)
          SET @CurrentCommand += ''''
          IF @ReadWriteFileGroups = 'Y' AND @CurrentDatabaseName <> 'master' SET @CurrentCommand += ', @read_write_filegroups = 1'
          IF @CompressionLevel IS NOT NULL SET @CurrentCommand += ', @compressionlevel = ' + CAST(@CompressionLevel AS nvarchar)
          IF @AdaptiveCompression IS NOT NULL SET @CurrentCommand += ', @adaptivecompression = ''' + CASE WHEN @AdaptiveCompression = 'SIZE' THEN 'Size' WHEN @AdaptiveCompression = 'SPEED' THEN 'Speed' END + ''''
          IF @BufferCount IS NOT NULL SET @CurrentCommand += ', @buffercount = ' + CAST(@BufferCount AS nvarchar)
          IF @CurrentMaxTransferSize IS NOT NULL SET @CurrentCommand += ', @maxtransfersize = ' + CAST(@CurrentMaxTransferSize AS nvarchar)
          IF @Threads IS NOT NULL SET @CurrentCommand += ', @threads = ' + CAST(@Threads AS nvarchar)
          IF @Init = 'Y' SET @CurrentCommand += ', @init = 1'
          IF @Format = 'Y' SET @CurrentCommand += ', @format = 1'
          IF @Throttle IS NOT NULL SET @CurrentCommand += ', @throttle = ' + CAST(@Throttle AS nvarchar)
          IF @Description IS NOT NULL SET @CurrentCommand += ', @desc = N''' + REPLACE(@Description,'''','''''') + ''''
          IF @ObjectLevelRecoveryMap = 'Y' SET @CurrentCommand += ', @olrmap = 1'

          IF @EncryptionAlgorithm IS NOT NULL SET @CurrentCommand += ', @cryptlevel = ' + CASE
          WHEN @EncryptionAlgorithm = 'RC2_40' THEN '0'
          WHEN @EncryptionAlgorithm = 'RC2_56' THEN '1'
          WHEN @EncryptionAlgorithm = 'RC2_112' THEN '2'
          WHEN @EncryptionAlgorithm = 'RC2_128' THEN '3'
          WHEN @EncryptionAlgorithm = 'TRIPLE_DES_3KEY' THEN '4'
          WHEN @EncryptionAlgorithm = 'RC4_128' THEN '5'
          WHEN @EncryptionAlgorithm = 'AES_128' THEN '6'
          WHEN @EncryptionAlgorithm = 'AES_192' THEN '7'
          WHEN @EncryptionAlgorithm = 'AES_256' THEN '8'
          END

          IF @EncryptionKey IS NOT NULL SET @CurrentCommand += ', @encryptionkey = N''' + REPLACE(@EncryptionKey,'''','''''') + ''''
          SET @CurrentCommand += ' IF @ReturnCode <> 0 RAISERROR(''Error performing LiteSpeed backup.'', 16, 1)'
        END

        IF @BackupSoftware = 'SQLBACKUP'
        BEGIN
          SET @CurrentDatabaseContext = 'master'

          SET @CurrentCommandType = 'sqlbackup'

          SELECT @CurrentCommand = CASE
          WHEN @CurrentBackupType IN('DIFF','FULL') THEN 'BACKUP DATABASE ' + QUOTENAME(@CurrentDatabaseName)
          WHEN @CurrentBackupType = 'LOG' THEN 'BACKUP LOG ' + QUOTENAME(@CurrentDatabaseName)
          END

          IF @ReadWriteFileGroups = 'Y' AND @CurrentDatabaseName <> 'master' SET @CurrentCommand += ' READ_WRITE_FILEGROUPS'

          SET @CurrentCommand += ' TO'

          SELECT @CurrentCommand += ' DISK = N''' + REPLACE(FilePath,'''','''''') + '''' + CASE WHEN ROW_NUMBER() OVER (ORDER BY FilePath ASC) <> @CurrentNumberOfFiles THEN ',' ELSE '' END
          FROM @CurrentFiles
          WHERE Mirror = 0
          ORDER BY FilePath ASC

          SET @CurrentCommand += ' WITH '

          IF EXISTS(SELECT * FROM @CurrentFiles WHERE Mirror = 1)
          BEGIN
            SET @CurrentCommand += ' MIRRORFILE' + ' = N''' + REPLACE((SELECT FilePath FROM @CurrentFiles WHERE Mirror = 1),'''','''''') + ''', '
          END

          IF @CheckSum = 'Y' SET @CurrentCommand += 'CHECKSUM'
          IF @CheckSum = 'N' SET @CurrentCommand += 'NO_CHECKSUM'
          IF @CurrentBackupType = 'DIFF' SET @CurrentCommand += ', DIFFERENTIAL'
          IF @CopyOnly = 'Y' SET @CurrentCommand += ', COPY_ONLY'
          IF @NoRecovery = 'Y' AND @CurrentBackupType = 'LOG' SET @CurrentCommand += ', NORECOVERY'
          IF @Init = 'Y' SET @CurrentCommand += ', INIT'
          IF @Format = 'Y' SET @CurrentCommand += ', FORMAT'
          IF @CompressionLevel IS NOT NULL SET @CurrentCommand += ', COMPRESSION = ' + CAST(@CompressionLevel AS nvarchar)
          IF @Threads IS NOT NULL SET @CurrentCommand += ', THREADCOUNT = ' + CAST(@Threads AS nvarchar)
          IF @CurrentMaxTransferSize IS NOT NULL SET @CurrentCommand += ', MAXTRANSFERSIZE = ' + CAST(@CurrentMaxTransferSize AS nvarchar)
          IF @Description IS NOT NULL SET @CurrentCommand += ', DESCRIPTION = N''' + REPLACE(@Description,'''','''''') + ''''

          IF @EncryptionAlgorithm IS NOT NULL SET @CurrentCommand += ', KEYSIZE = ' + CASE
          WHEN @EncryptionAlgorithm = 'AES_128' THEN '128'
          WHEN @EncryptionAlgorithm = 'AES_256' THEN '256'
          END

          IF @EncryptionKey IS NOT NULL SET @CurrentCommand += ', PASSWORD = N''' + REPLACE(@EncryptionKey,'''','''''') + ''''
          SET @CurrentCommand = 'DECLARE @ReturnCode int EXECUTE @ReturnCode = dbo.sqlbackup N''-SQL "' + REPLACE(@CurrentCommand,'''','''''') + '"''' + ' IF @ReturnCode <> 0 RAISERROR(''Error performing SQLBackup backup.'', 16, 1)'
        END

        IF @BackupSoftware = 'SQLSAFE'
        BEGIN
          SET @CurrentDatabaseContext = 'master'

          SET @CurrentCommandType = 'xp_ss_backup'

          SET @CurrentCommand = 'DECLARE @ReturnCode int EXECUTE @ReturnCode = dbo.xp_ss_backup @database = N''' + REPLACE(@CurrentDatabaseName,'''','''''') + ''''

          SELECT @CurrentCommand += ', ' + CASE WHEN ROW_NUMBER() OVER (ORDER BY FilePath ASC) = 1 THEN '@filename' ELSE '@backupfile' END + ' = N''' + REPLACE(FilePath,'''','''''') + ''''
          FROM @CurrentFiles
          WHERE Mirror = 0
          ORDER BY FilePath ASC

          SELECT @CurrentCommand += ', @mirrorfile = N''' + REPLACE(FilePath,'''','''''') + ''''
          FROM @CurrentFiles
          WHERE Mirror = 1
          ORDER BY FilePath ASC

          SET @CurrentCommand += ', @backuptype = ' + CASE WHEN @CurrentBackupType = 'FULL' THEN '''Full''' WHEN @CurrentBackupType = 'DIFF' THEN '''Differential''' WHEN @CurrentBackupType = 'LOG' THEN '''Log''' END
          IF @ReadWriteFileGroups = 'Y' AND @CurrentDatabaseName <> 'master' SET @CurrentCommand += ', @readwritefilegroups = 1'
          SET @CurrentCommand += ', @checksum = ' + CASE WHEN @CheckSum = 'Y' THEN '1' WHEN @CheckSum = 'N' THEN '0' END
          SET @CurrentCommand += ', @copyonly = ' + CASE WHEN @CopyOnly = 'Y' THEN '1' WHEN @CopyOnly = 'N' THEN '0' END
          IF @CompressionLevel IS NOT NULL SET @CurrentCommand += ', @compressionlevel = ' + CAST(@CompressionLevel AS nvarchar)
          IF @Threads IS NOT NULL SET @CurrentCommand += ', @threads = ' + CAST(@Threads AS nvarchar)
          IF @Init = 'Y' SET @CurrentCommand += ', @overwrite = 1'
          IF @Description IS NOT NULL SET @CurrentCommand += ', @desc = N''' + REPLACE(@Description,'''','''''') + ''''

          IF @EncryptionAlgorithm IS NOT NULL SET @CurrentCommand += ', @encryptiontype = N''' + CASE
          WHEN @EncryptionAlgorithm = 'AES_128' THEN 'AES128'
          WHEN @EncryptionAlgorithm = 'AES_256' THEN 'AES256'
          END + ''''

          IF @EncryptionKey IS NOT NULL SET @CurrentCommand += ', @encryptedbackuppassword = N''' + REPLACE(@EncryptionKey,'''','''''') + ''''
          SET @CurrentCommand += ' IF @ReturnCode <> 0 RAISERROR(''Error performing SQLsafe backup.'', 16, 1)'
        END

        IF @BackupSoftware = 'DATA_DOMAIN_BOOST'
        BEGIN
          SET @CurrentDatabaseContext = 'master'

          SET @CurrentCommandType = 'emc_run_backup'

          SET @CurrentCommand = 'DECLARE @ReturnCode int EXECUTE @ReturnCode = dbo.emc_run_backup '''

          SET @CurrentCommand += ' -c ' + CASE WHEN @CurrentAvailabilityGroup IS NOT NULL THEN @Cluster ELSE CAST(SERVERPROPERTY('MachineName') AS nvarchar) END

          SET @CurrentCommand += ' -l ' + CASE
          WHEN @CurrentBackupType = 'FULL' THEN 'full'
          WHEN @CurrentBackupType = 'DIFF' THEN 'diff'
          WHEN @CurrentBackupType = 'LOG' THEN 'incr'
          END

          IF @NoRecovery = 'Y' SET @CurrentCommand += ' -H'

          IF @CleanupTime IS NOT NULL SET @CurrentCommand += ' -y +' + CAST(@CleanupTime/24 + CASE WHEN @CleanupTime%24 > 0 THEN 1 ELSE 0 END AS nvarchar) + 'd'

          IF @CheckSum = 'Y' SET @CurrentCommand += ' -k'

          SET @CurrentCommand += ' -S ' + CAST(@CurrentNumberOfFiles AS nvarchar)

          IF @Description IS NOT NULL SET @CurrentCommand += ' -b "' + REPLACE(@Description,'''','''''') + '"'

          IF @BufferCount IS NOT NULL SET @CurrentCommand += ' -O "BUFFERCOUNT=' + CAST(@BufferCount AS nvarchar) + '"'

          IF @ReadWriteFileGroups = 'Y' AND @CurrentDatabaseName <> 'master' SET @CurrentCommand += ' -O "READ_WRITE_FILEGROUPS"'

          IF @DataDomainBoostHost IS NOT NULL SET @CurrentCommand += ' -a "NSR_DFA_SI_DD_HOST=' + REPLACE(@DataDomainBoostHost,'''','''''') + '"'
          IF @DataDomainBoostUser IS NOT NULL SET @CurrentCommand += ' -a "NSR_DFA_SI_DD_USER=' + REPLACE(@DataDomainBoostUser,'''','''''') + '"'
          IF @DataDomainBoostDevicePath IS NOT NULL SET @CurrentCommand += ' -a "NSR_DFA_SI_DEVICE_PATH=' + REPLACE(@DataDomainBoostDevicePath,'''','''''') + '"'
          IF @DataDomainBoostLockboxPath IS NOT NULL SET @CurrentCommand += ' -a "NSR_DFA_SI_DD_LOCKBOX_PATH=' + REPLACE(@DataDomainBoostLockboxPath,'''','''''') + '"'
          SET @CurrentCommand += ' -a "NSR_SKIP_NON_BACKUPABLE_STATE_DB=TRUE"'
          SET @CurrentCommand += ' -a "BACKUP_PROMOTION=NONE"'
          IF @CopyOnly = 'Y' SET @CurrentCommand += ' -a "NSR_COPY_ONLY=TRUE"'

          IF SERVERPROPERTY('InstanceName') IS NULL SET @CurrentCommand += ' "MSSQL' + ':' + REPLACE(REPLACE(@CurrentDatabaseName,'''',''''''),'.','\.') + '"'
          IF SERVERPROPERTY('InstanceName') IS NOT NULL SET @CurrentCommand += ' "MSSQL$' + CAST(SERVERPROPERTY('InstanceName') AS nvarchar) + ':' + REPLACE(REPLACE(@CurrentDatabaseName,'''',''''''),'.','\.') + '"'

          SET @CurrentCommand += ''''

          SET @CurrentCommand += ' IF @ReturnCode <> 0 RAISERROR(''Error performing Data Domain Boost backup.'', 16, 1)'
        END

        EXECUTE @CurrentCommandOutput = dbo.CommandExecute @DatabaseContext = @CurrentDatabaseContext, @Command = @CurrentCommand, @CommandType = @CurrentCommandType, @Mode = 1, @DatabaseName = @CurrentDatabaseName, @LogToTable = @LogToTable, @Execute = @Execute
        SET @Error = @@ERROR
        IF @Error <> 0 SET @CurrentCommandOutput = @Error
        IF @CurrentCommandOutput <> 0 SET @ReturnCode = @CurrentCommandOutput
        SET @CurrentBackupOutput = @CurrentCommandOutput
      END

      -- Verify the backup
      IF @CurrentBackupOutput = 0 AND @Verify = 'Y'
      BEGIN
        WHILE (1 = 1)
        BEGIN
          SELECT TOP 1 @CurrentBackupSetID = ID,
                       @CurrentIsMirror = Mirror
          FROM @CurrentBackupSet
          WHERE VerifyCompleted = 0
          ORDER BY ID ASC

          IF @@ROWCOUNT = 0
          BEGIN
            BREAK
          END

          IF @BackupSoftware IS NULL
          BEGIN
            SET @CurrentDatabaseContext = 'master'

            SET @CurrentCommandType = 'RESTORE_VERIFYONLY'

            SET @CurrentCommand = 'RESTORE VERIFYONLY FROM'

            SELECT @CurrentCommand += ' ' + [Type] + ' = N''' + REPLACE(FilePath,'''','''''') + '''' + CASE WHEN ROW_NUMBER() OVER (ORDER BY FilePath ASC) <> @CurrentNumberOfFiles THEN ',' ELSE '' END
            FROM @CurrentFiles
            WHERE Mirror = @CurrentIsMirror
            ORDER BY FilePath ASC

            SET @CurrentCommand += ' WITH '
            IF @CheckSum = 'Y' SET @CurrentCommand += 'CHECKSUM'
            IF @CheckSum = 'N' SET @CurrentCommand += 'NO_CHECKSUM'
            IF @URL IS NOT NULL AND @Credential IS NOT NULL SET @CurrentCommand += ', CREDENTIAL = N''' + REPLACE(@Credential,'''','''''') + ''''
          END

          IF @BackupSoftware = 'LITESPEED'
          BEGIN
            SET @CurrentDatabaseContext = 'master'

            SET @CurrentCommandType = 'xp_restore_verifyonly'

            SET @CurrentCommand = 'DECLARE @ReturnCode int EXECUTE @ReturnCode = dbo.xp_restore_verifyonly'

            SELECT @CurrentCommand += ' @filename = N''' + REPLACE(FilePath,'''','''''') + '''' + CASE WHEN ROW_NUMBER() OVER (ORDER BY FilePath ASC) <> @CurrentNumberOfFiles THEN ',' ELSE '' END
            FROM @CurrentFiles
            WHERE Mirror = @CurrentIsMirror
            ORDER BY FilePath ASC

            SET @CurrentCommand += ', @with = '''
            IF @CheckSum = 'Y' SET @CurrentCommand += 'CHECKSUM'
            IF @CheckSum = 'N' SET @CurrentCommand += 'NO_CHECKSUM'
            SET @CurrentCommand += ''''
            IF @EncryptionKey IS NOT NULL SET @CurrentCommand += ', @encryptionkey = N''' + REPLACE(@EncryptionKey,'''','''''') + ''''

            SET @CurrentCommand += ' IF @ReturnCode <> 0 RAISERROR(''Error verifying LiteSpeed backup.'', 16, 1)'
          END

          IF @BackupSoftware = 'SQLBACKUP'
          BEGIN
            SET @CurrentDatabaseContext = 'master'

            SET @CurrentCommandType = 'sqlbackup'

            SET @CurrentCommand = 'RESTORE VERIFYONLY FROM'

            SELECT @CurrentCommand += ' DISK = N''' + REPLACE(FilePath,'''','''''') + '''' + CASE WHEN ROW_NUMBER() OVER (ORDER BY FilePath ASC) <> @CurrentNumberOfFiles THEN ',' ELSE '' END
            FROM @CurrentFiles
            WHERE Mirror = @CurrentIsMirror
            ORDER BY FilePath ASC

            SET @CurrentCommand += ' WITH '
            IF @CheckSum = 'Y' SET @CurrentCommand += 'CHECKSUM'
            IF @CheckSum = 'N' SET @CurrentCommand += 'NO_CHECKSUM'
            IF @EncryptionKey IS NOT NULL SET @CurrentCommand += ', PASSWORD = N''' + REPLACE(@EncryptionKey,'''','''''') + ''''

            SET @CurrentCommand = 'DECLARE @ReturnCode int EXECUTE @ReturnCode = dbo.sqlbackup N''-SQL "' + REPLACE(@CurrentCommand,'''','''''') + '"''' + ' IF @ReturnCode <> 0 RAISERROR(''Error verifying SQLBackup backup.'', 16, 1)'
          END

          IF @BackupSoftware = 'SQLSAFE'
          BEGIN
            SET @CurrentDatabaseContext = 'master'

            SET @CurrentCommandType = 'xp_ss_verify'

            SET @CurrentCommand = 'DECLARE @ReturnCode int EXECUTE @ReturnCode = dbo.xp_ss_verify @database = N''' + REPLACE(@CurrentDatabaseName,'''','''''') + ''''

            SELECT @CurrentCommand += ', ' + CASE WHEN ROW_NUMBER() OVER (ORDER BY FilePath ASC) = 1 THEN '@filename' ELSE '@backupfile' END + ' = N''' + REPLACE(FilePath,'''','''''') + ''''
            FROM @CurrentFiles
            WHERE Mirror = @CurrentIsMirror
            ORDER BY FilePath ASC

            SET @CurrentCommand += ' IF @ReturnCode <> 0 RAISERROR(''Error verifying SQLsafe backup.'', 16, 1)'
          END

          EXECUTE @CurrentCommandOutput = dbo.CommandExecute @DatabaseContext = @CurrentDatabaseContext, @Command = @CurrentCommand, @CommandType = @CurrentCommandType, @Mode = 1, @DatabaseName = @CurrentDatabaseName, @LogToTable = @LogToTable, @Execute = @Execute
          SET @Error = @@ERROR
          IF @Error <> 0 SET @CurrentCommandOutput = @Error
          IF @CurrentCommandOutput <> 0 SET @ReturnCode = @CurrentCommandOutput

          UPDATE @CurrentBackupSet
          SET VerifyCompleted = 1,
              VerifyOutput = @CurrentCommandOutput
          WHERE ID = @CurrentBackupSetID

          SET @CurrentBackupSetID = NULL
          SET @CurrentIsMirror = NULL

          SET @CurrentDatabaseContext = NULL
          SET @CurrentCommand = NULL
          SET @CurrentCommandOutput = NULL
          SET @CurrentCommandType = NULL
        END
      END

      IF @CleanupMode = 'AFTER_BACKUP'
      BEGIN
        INSERT INTO @CurrentCleanupDates (CleanupDate, Mirror)
        SELECT DATEADD(hh,-(@CleanupTime),SYSDATETIME()), 0

        IF NOT EXISTS(SELECT * FROM @CurrentCleanupDates WHERE (Mirror = 0 OR Mirror IS NULL) AND CleanupDate IS NULL)
        BEGIN
          UPDATE @CurrentDirectories
          SET CleanupDate = (SELECT MIN(CleanupDate)
                             FROM @CurrentCleanupDates
                             WHERE (Mirror = 0 OR Mirror IS NULL)),
              CleanupMode = 'AFTER_BACKUP'
          WHERE Mirror = 0
        END
      END

      IF @MirrorCleanupMode = 'AFTER_BACKUP'
      BEGIN
        INSERT INTO @CurrentCleanupDates (CleanupDate, Mirror)
        SELECT DATEADD(hh,-(@MirrorCleanupTime),SYSDATETIME()), 1

        IF NOT EXISTS(SELECT * FROM @CurrentCleanupDates WHERE (Mirror = 1 OR Mirror IS NULL) AND CleanupDate IS NULL)
        BEGIN
          UPDATE @CurrentDirectories
          SET CleanupDate = (SELECT MIN(CleanupDate)
                             FROM @CurrentCleanupDates
                             WHERE (Mirror = 1 OR Mirror IS NULL)),
              CleanupMode = 'AFTER_BACKUP'
          WHERE Mirror = 1
        END
      END

      -- Delete old backup files, after backup
      IF ((@CurrentBackupOutput = 0 AND @Verify = 'N')
      OR (@CurrentBackupOutput = 0 AND @Verify = 'Y' AND NOT EXISTS (SELECT * FROM @CurrentBackupSet WHERE VerifyOutput <> 0 OR VerifyOutput IS NULL)))
      AND (@BackupSoftware <> 'DATA_DOMAIN_BOOST' OR @BackupSoftware IS NULL)
      AND @CurrentBackupType = @BackupType
      BEGIN
        WHILE (1 = 1)
        BEGIN
          SELECT TOP 1 @CurrentDirectoryID = ID,
                       @CurrentDirectoryPath = DirectoryPath,
                       @CurrentCleanupDate = CleanupDate
          FROM @CurrentDirectories
          WHERE CleanupDate IS NOT NULL
          AND CleanupMode = 'AFTER_BACKUP'
          AND CleanupCompleted = 0
          ORDER BY ID ASC

          IF @@ROWCOUNT = 0
          BEGIN
            BREAK
          END

          IF @BackupSoftware IS NULL
          BEGIN
            SET @CurrentDatabaseContext = 'master'

            SET @CurrentCommandType = 'xp_delete_file'

            SET @CurrentCommand = 'DECLARE @ReturnCode int EXECUTE @ReturnCode = dbo.xp_delete_file 0, N''' + REPLACE(@CurrentDirectoryPath,'''','''''') + ''', ''' + @CurrentFileExtension + ''', ''' + CONVERT(nvarchar(19),@CurrentCleanupDate,126) + ''' IF @ReturnCode <> 0 RAISERROR(''Error deleting files.'', 16, 1)'
          END

          IF @BackupSoftware = 'LITESPEED'
          BEGIN
            SET @CurrentDatabaseContext = 'master'

            SET @CurrentCommandType = 'xp_slssqlmaint'

            SET @CurrentCommand = 'DECLARE @ReturnCode int EXECUTE @ReturnCode = dbo.xp_slssqlmaint N''-MAINTDEL -DELFOLDER "' + REPLACE(@CurrentDirectoryPath,'''','''''') + '" -DELEXTENSION "' + @CurrentFileExtension + '" -DELUNIT "' + CAST(DATEDIFF(mi,@CurrentCleanupDate,SYSDATETIME()) + 1 AS nvarchar) + '" -DELUNITTYPE "minutes" -DELUSEAGE'' IF @ReturnCode <> 0 RAISERROR(''Error deleting LiteSpeed backup files.'', 16, 1)'
          END

          IF @BackupSoftware = 'SQLBACKUP'
          BEGIN
            SET @CurrentDatabaseContext = 'master'

            SET @CurrentCommandType = 'sqbutility'

            SET @CurrentCommand = 'DECLARE @ReturnCode int EXECUTE @ReturnCode = dbo.sqbutility 1032, N''' + REPLACE(@CurrentDatabaseName,'''','''''') + ''', N''' + REPLACE(@CurrentDirectoryPath,'''','''''') + ''', ''' + CASE WHEN @CurrentBackupType = 'FULL' THEN 'D' WHEN @CurrentBackupType = 'DIFF' THEN 'I' WHEN @CurrentBackupType = 'LOG' THEN 'L' END + ''', ''' + CAST(DATEDIFF(hh,@CurrentCleanupDate,SYSDATETIME()) + 1 AS nvarchar) + 'h'', ' + ISNULL('''' + REPLACE(@EncryptionKey,'''','''''') + '''','NULL') + ' IF @ReturnCode <> 0 RAISERROR(''Error deleting SQLBackup backup files.'', 16, 1)'
          END

          IF @BackupSoftware = 'SQLSAFE'
          BEGIN
            SET @CurrentDatabaseContext = 'master'

            SET @CurrentCommandType = 'xp_ss_delete'

            SET @CurrentCommand = 'DECLARE @ReturnCode int EXECUTE @ReturnCode = dbo.xp_ss_delete @filename = N''' + REPLACE(@CurrentDirectoryPath,'''','''''') + '\*.' + @CurrentFileExtension + ''', @age = ''' + CAST(DATEDIFF(mi,@CurrentCleanupDate,SYSDATETIME()) + 1 AS nvarchar) + 'Minutes'' IF @ReturnCode <> 0 RAISERROR(''Error deleting SQLsafe backup files.'', 16, 1)'
          END

          EXECUTE @CurrentCommandOutput = dbo.CommandExecute @DatabaseContext = @CurrentDatabaseContext, @Command = @CurrentCommand, @CommandType = @CurrentCommandType, @Mode = 1, @DatabaseName = @CurrentDatabaseName, @LogToTable = @LogToTable, @Execute = @Execute
          SET @Error = @@ERROR
          IF @Error <> 0 SET @CurrentCommandOutput = @Error
          IF @CurrentCommandOutput <> 0 SET @ReturnCode = @CurrentCommandOutput

          UPDATE @CurrentDirectories
          SET CleanupCompleted = 1,
              CleanupOutput = @CurrentCommandOutput
          WHERE ID = @CurrentDirectoryID

          SET @CurrentDirectoryID = NULL
          SET @CurrentDirectoryPath = NULL
          SET @CurrentCleanupDate = NULL

          SET @CurrentDatabaseContext = NULL
          SET @CurrentCommand = NULL
          SET @CurrentCommandOutput = NULL
          SET @CurrentCommandType = NULL
        END
      END
    END

    IF @CurrentDatabaseState = 'SUSPECT'
    BEGIN
      SET @ErrorMessage = 'The database ' + QUOTENAME(@CurrentDatabaseName) + ' is in a SUSPECT state.'
      RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
      SET @Error = @@ERROR
      RAISERROR(@EmptyLine,10,1) WITH NOWAIT
    END

    -- Update that the database is completed
    IF @DatabasesInParallel = 'Y'
    BEGIN
      UPDATE dbo.QueueDatabase
      SET DatabaseEndTime = SYSDATETIME()
      WHERE QueueID = @QueueID
      AND DatabaseName = @CurrentDatabaseName
    END
    ELSE
    BEGIN
      UPDATE @tmpDatabases
      SET Completed = 1
      WHERE Selected = 1
      AND Completed = 0
      AND ID = @CurrentDBID
    END

    -- Clear variables
    SET @CurrentDBID = NULL
    SET @CurrentDatabaseName = NULL

    SET @CurrentDatabase_sp_executesql = NULL

    SET @CurrentUserAccess = NULL
    SET @CurrentIsReadOnly = NULL
    SET @CurrentDatabaseState = NULL
    SET @CurrentInStandby = NULL
    SET @CurrentRecoveryModel = NULL
    SET @CurrentIsEncrypted = NULL
    SET @CurrentDatabaseSize = NULL

    SET @CurrentBackupType = NULL
    SET @CurrentMaxTransferSize = NULL
    SET @CurrentNumberOfFiles = NULL
    SET @CurrentFileExtension = NULL
    SET @CurrentFileNumber = NULL
    SET @CurrentDifferentialBaseLSN = NULL
    SET @CurrentDifferentialBaseIsSnapshot = NULL
    SET @CurrentLogLSN = NULL
    SET @CurrentLatestBackup = NULL
    SET @CurrentDatabaseNameFS = NULL
    SET @CurrentDirectoryStructure = NULL
    SET @CurrentDatabaseFileName = NULL
    SET @CurrentMaxFilePathLength = NULL
    SET @CurrentDate = NULL
    SET @CurrentCleanupDate = NULL
    SET @CurrentIsDatabaseAccessible = NULL
    SET @CurrentReplicaID = NULL
    SET @CurrentAvailabilityGroupID = NULL
    SET @CurrentAvailabilityGroup = NULL
    SET @CurrentAvailabilityGroupRole = NULL
    SET @CurrentAvailabilityGroupBackupPreference = NULL
    SET @CurrentIsPreferredBackupReplica = NULL
    SET @CurrentDatabaseMirroringRole = NULL
    SET @CurrentLogShippingRole = NULL
    SET @CurrentLastLogBackup = NULL
    SET @CurrentLogSizeSinceLastLogBackup = NULL
    SET @CurrentAllocatedExtentPageCount = NULL
    SET @CurrentModifiedExtentPageCount = NULL

    SET @CurrentDatabaseContext = NULL
    SET @CurrentCommand = NULL
    SET @CurrentCommandOutput = NULL
    SET @CurrentCommandType = NULL

    SET @CurrentBackupOutput = NULL

    DELETE FROM @CurrentDirectories
    DELETE FROM @CurrentURLs
    DELETE FROM @CurrentFiles
    DELETE FROM @CurrentCleanupDates
    DELETE FROM @CurrentBackupSet

  END

  ----------------------------------------------------------------------------------------------------
  --// Log completing information                                                                 //--
  ----------------------------------------------------------------------------------------------------

  Logging:
  SET @EndMessage = 'Date and time: ' + CONVERT(nvarchar,SYSDATETIME(),120)
  RAISERROR('%s',10,1,@EndMessage) WITH NOWAIT

  RAISERROR(@EmptyLine,10,1) WITH NOWAIT

  IF @ReturnCode <> 0
  BEGIN
    RETURN @ReturnCode
  END

  ----------------------------------------------------------------------------------------------------

END
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[DatabaseIntegrityCheck]') AND type in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [dbo].[DatabaseIntegrityCheck] AS'
END
GO
ALTER PROCEDURE [dbo].[DatabaseIntegrityCheck]

@Databases nvarchar(max) = NULL,
@CheckCommands nvarchar(max) = 'CHECKDB',
@PhysicalOnly nvarchar(max) = 'N',
@DataPurity nvarchar(max) = 'N',
@NoIndex nvarchar(max) = 'N',
@ExtendedLogicalChecks nvarchar(max) = 'N',
@TabLock nvarchar(max) = 'N',
@FileGroups nvarchar(max) = NULL,
@Objects nvarchar(max) = NULL,
@MaxDOP int = NULL,
@AvailabilityGroups nvarchar(max) = NULL,
@AvailabilityGroupReplicas nvarchar(max) = 'ALL',
@Updateability nvarchar(max) = 'ALL',
@TimeLimit int = NULL,
@LockTimeout int = NULL,
@LockMessageSeverity int = 16,
@StringDelimiter nvarchar(max) = ',',
@DatabaseOrder nvarchar(max) = NULL,
@DatabasesInParallel nvarchar(max) = 'N',
@LogToTable nvarchar(max) = 'N',
@Execute nvarchar(max) = 'Y'

AS

BEGIN

  ----------------------------------------------------------------------------------------------------
  --// Source:  Database Maintenance Solution provided by Ascent Technology PTY (Ltd) //--
  --// Version: 2022-12-03 17:23:44                                                              //--
  ----------------------------------------------------------------------------------------------------

  SET NOCOUNT ON

  DECLARE @StartMessage nvarchar(max)
  DECLARE @EndMessage nvarchar(max)
  DECLARE @DatabaseMessage nvarchar(max)
  DECLARE @ErrorMessage nvarchar(max)
  DECLARE @Severity int

  DECLARE @StartTime datetime2 = SYSDATETIME()
  DECLARE @SchemaName nvarchar(max) = OBJECT_SCHEMA_NAME(@@PROCID)
  DECLARE @ObjectName nvarchar(max) = OBJECT_NAME(@@PROCID)
  DECLARE @VersionTimestamp nvarchar(max) = SUBSTRING(OBJECT_DEFINITION(@@PROCID),CHARINDEX('--// Version: ',OBJECT_DEFINITION(@@PROCID)) + LEN('--// Version: ') + 1, 19)
  DECLARE @Parameters nvarchar(max)

  DECLARE @HostPlatform nvarchar(max)

  DECLARE @QueueID int
  DECLARE @QueueStartTime datetime2

  DECLARE @CurrentDBID int
  DECLARE @CurrentDatabaseName nvarchar(max)

  DECLARE @CurrentDatabase_sp_executesql nvarchar(max)

  DECLARE @CurrentUserAccess nvarchar(max)
  DECLARE @CurrentIsReadOnly bit
  DECLARE @CurrentDatabaseState nvarchar(max)
  DECLARE @CurrentInStandby bit
  DECLARE @CurrentRecoveryModel nvarchar(max)

  DECLARE @CurrentIsDatabaseAccessible bit
  DECLARE @CurrentReplicaID uniqueidentifier
  DECLARE @CurrentAvailabilityGroupID uniqueidentifier
  DECLARE @CurrentAvailabilityGroup nvarchar(max)
  DECLARE @CurrentAvailabilityGroupRole nvarchar(max)
  DECLARE @CurrentAvailabilityGroupBackupPreference nvarchar(max)
  DECLARE @CurrentSecondaryRoleAllowConnections nvarchar(max)
  DECLARE @CurrentIsPreferredBackupReplica bit
  DECLARE @CurrentDatabaseMirroringRole nvarchar(max)

  DECLARE @CurrentFGID int
  DECLARE @CurrentFileGroupID int
  DECLARE @CurrentFileGroupName nvarchar(max)
  DECLARE @CurrentFileGroupExists bit

  DECLARE @CurrentOID int
  DECLARE @CurrentSchemaID int
  DECLARE @CurrentSchemaName nvarchar(max)
  DECLARE @CurrentObjectID int
  DECLARE @CurrentObjectName nvarchar(max)
  DECLARE @CurrentObjectType nvarchar(max)
  DECLARE @CurrentObjectExists bit

  DECLARE @CurrentDatabaseContext nvarchar(max)
  DECLARE @CurrentCommand nvarchar(max)
  DECLARE @CurrentCommandOutput int
  DECLARE @CurrentCommandType nvarchar(max)

  DECLARE @Errors TABLE (ID int IDENTITY PRIMARY KEY,
                         [Message] nvarchar(max) NOT NULL,
                         Severity int NOT NULL,
                         [State] int)

  DECLARE @CurrentMessage nvarchar(max)
  DECLARE @CurrentSeverity int
  DECLARE @CurrentState int

  DECLARE @tmpDatabases TABLE (ID int IDENTITY,
                               DatabaseName nvarchar(max),
                               DatabaseType nvarchar(max),
                               AvailabilityGroup bit,
                               [Snapshot] bit,
                               StartPosition int,
                               LastCommandTime datetime2,
                               DatabaseSize bigint,
                               LastGoodCheckDbTime datetime2,
                               [Order] int,
                               Selected bit,
                               Completed bit,
                               PRIMARY KEY(Selected, Completed, [Order], ID))

  DECLARE @tmpAvailabilityGroups TABLE (ID int IDENTITY PRIMARY KEY,
                                        AvailabilityGroupName nvarchar(max),
                                        StartPosition int,
                                        Selected bit)

  DECLARE @tmpDatabasesAvailabilityGroups TABLE (DatabaseName nvarchar(max),
                                                 AvailabilityGroupName nvarchar(max))

  DECLARE @tmpFileGroups TABLE (ID int IDENTITY,
                                FileGroupID int,
                                FileGroupName nvarchar(max),
                                StartPosition int,
                                [Order] int,
                                Selected bit,
                                Completed bit,
                                PRIMARY KEY(Selected, Completed, [Order], ID))

  DECLARE @tmpObjects TABLE (ID int IDENTITY,
                             SchemaID int,
                             SchemaName nvarchar(max),
                             ObjectID int,
                             ObjectName nvarchar(max),
                             ObjectType nvarchar(max),
                             StartPosition int,
                             [Order] int,
                             Selected bit,
                             Completed bit,
                             PRIMARY KEY(Selected, Completed, [Order], ID))

  DECLARE @SelectedDatabases TABLE (DatabaseName nvarchar(max),
                                    DatabaseType nvarchar(max),
                                    AvailabilityGroup nvarchar(max),
                                    StartPosition int,
                                    Selected bit)

  DECLARE @SelectedAvailabilityGroups TABLE (AvailabilityGroupName nvarchar(max),
                                             StartPosition int,
                                             Selected bit)

  DECLARE @SelectedFileGroups TABLE (DatabaseName nvarchar(max),
                                     FileGroupName nvarchar(max),
                                     StartPosition int,
                                     Selected bit)

  DECLARE @SelectedObjects TABLE (DatabaseName nvarchar(max),
                                  SchemaName nvarchar(max),
                                  ObjectName nvarchar(max),
                                  StartPosition int,
                                  Selected bit)

  DECLARE @SelectedCheckCommands TABLE (CheckCommand nvarchar(max))

  DECLARE @Error int = 0
  DECLARE @ReturnCode int = 0

  DECLARE @EmptyLine nvarchar(max) = CHAR(9)

  DECLARE @Version numeric(18,10) = CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max)),CHARINDEX('.',CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max))) - 1) + '.' + REPLACE(RIGHT(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max)), LEN(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max))) - CHARINDEX('.',CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max)))),'.','') AS numeric(18,10))

  IF @Version >= 14
  BEGIN
    SELECT @HostPlatform = host_platform
    FROM sys.dm_os_host_info
  END
  ELSE
  BEGIN
    SET @HostPlatform = 'Windows'
  END

  DECLARE @AmazonRDS bit = CASE WHEN DB_ID('rdsadmin') IS NOT NULL AND SUSER_SNAME(0x01) = 'rdsa' THEN 1 ELSE 0 END

  ----------------------------------------------------------------------------------------------------
  --// Log initial information                                                                    //--
  ----------------------------------------------------------------------------------------------------

  SET @Parameters = '@Databases = ' + ISNULL('''' + REPLACE(@Databases,'''','''''') + '''','NULL')
  SET @Parameters += ', @CheckCommands = ' + ISNULL('''' + REPLACE(@CheckCommands,'''','''''') + '''','NULL')
  SET @Parameters += ', @PhysicalOnly = ' + ISNULL('''' + REPLACE(@PhysicalOnly,'''','''''') + '''','NULL')
  SET @Parameters += ', @DataPurity = ' + ISNULL('''' + REPLACE(@DataPurity,'''','''''') + '''','NULL')
  SET @Parameters += ', @NoIndex = ' + ISNULL('''' + REPLACE(@NoIndex,'''','''''') + '''','NULL')
  SET @Parameters += ', @ExtendedLogicalChecks = ' + ISNULL('''' + REPLACE(@ExtendedLogicalChecks,'''','''''') + '''','NULL')
  SET @Parameters += ', @TabLock = ' + ISNULL('''' + REPLACE(@TabLock,'''','''''') + '''','NULL')
  SET @Parameters += ', @FileGroups = ' + ISNULL('''' + REPLACE(@FileGroups,'''','''''') + '''','NULL')
  SET @Parameters += ', @Objects = ' + ISNULL('''' + REPLACE(@Objects,'''','''''') + '''','NULL')
  SET @Parameters += ', @MaxDOP = ' + ISNULL(CAST(@MaxDOP AS nvarchar),'NULL')
  SET @Parameters += ', @AvailabilityGroups = ' + ISNULL('''' + REPLACE(@AvailabilityGroups,'''','''''') + '''','NULL')
  SET @Parameters += ', @AvailabilityGroupReplicas = ' + ISNULL('''' + REPLACE(@AvailabilityGroupReplicas,'''','''''') + '''','NULL')
  SET @Parameters += ', @Updateability = ' + ISNULL('''' + REPLACE(@Updateability,'''','''''') + '''','NULL')
  SET @Parameters += ', @TimeLimit = ' + ISNULL(CAST(@TimeLimit AS nvarchar),'NULL')
  SET @Parameters += ', @LockTimeout = ' + ISNULL(CAST(@LockTimeout AS nvarchar),'NULL')
  SET @Parameters += ', @LockMessageSeverity = ' + ISNULL(CAST(@LockMessageSeverity AS nvarchar),'NULL')
  SET @Parameters += ', @StringDelimiter = ' + ISNULL('''' + REPLACE(@StringDelimiter,'''','''''') + '''','NULL')
  SET @Parameters += ', @DatabaseOrder = ' + ISNULL('''' + REPLACE(@DatabaseOrder,'''','''''') + '''','NULL')
  SET @Parameters += ', @DatabasesInParallel = ' + ISNULL('''' + REPLACE(@DatabasesInParallel,'''','''''') + '''','NULL')
  SET @Parameters += ', @LogToTable = ' + ISNULL('''' + REPLACE(@LogToTable,'''','''''') + '''','NULL')
  SET @Parameters += ', @Execute = ' + ISNULL('''' + REPLACE(@Execute,'''','''''') + '''','NULL')

  SET @StartMessage = 'Date and time: ' + CONVERT(nvarchar,@StartTime,120)
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Server: ' + CAST(SERVERPROPERTY('ServerName') AS nvarchar(max))
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Version: ' + CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max))
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Edition: ' + CAST(SERVERPROPERTY('Edition') AS nvarchar(max))
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Platform: ' + @HostPlatform
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Procedure: ' + QUOTENAME(DB_NAME(DB_ID())) + '.' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@ObjectName)
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Parameters: ' + @Parameters
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Version: ' + @VersionTimestamp
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Source: Ascent Technology. https://www.ascent.tech '
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  RAISERROR(@EmptyLine,10,1) WITH NOWAIT

  ----------------------------------------------------------------------------------------------------
  --// Check core requirements                                                                    //--
  ----------------------------------------------------------------------------------------------------

  IF NOT (SELECT [compatibility_level] FROM sys.databases WHERE database_id = DB_ID()) >= 90
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT  'The database ' + QUOTENAME(DB_NAME(DB_ID())) + ' has to be in compatibility level 90 or higher.', 16, 1
  END

  IF NOT (SELECT uses_ansi_nulls FROM sys.sql_modules WHERE [object_id] = @@PROCID) = 1
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'ANSI_NULLS has to be set to ON for the stored procedure.', 16, 1
  END

  IF NOT (SELECT uses_quoted_identifier FROM sys.sql_modules WHERE [object_id] = @@PROCID) = 1
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'QUOTED_IDENTIFIER has to be set to ON for the stored procedure.', 16, 1
  END

  IF NOT EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'P' AND schemas.[name] = 'dbo' AND objects.[name] = 'CommandExecute')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The stored procedure CommandExecute is missing. Contact your Ascent Technology Consultant. https://www.ascent.tech ', 16, 1
  END

  IF EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'P' AND schemas.[name] = 'dbo' AND objects.[name] = 'CommandExecute' AND OBJECT_DEFINITION(objects.[object_id]) NOT LIKE '%@DatabaseContext%')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The stored procedure CommandExecute needs to be updated. Contact your Ascent Technology Consultant. https://www.ascent.tech ', 16, 1
  END

  IF @LogToTable = 'Y' AND NOT EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'U' AND schemas.[name] = 'dbo' AND objects.[name] = 'CommandLog')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The table CommandLog is missing. Contact your Ascent Technology Consultant. https://www.ascent.tech ', 16, 1
  END

  IF @DatabasesInParallel = 'Y' AND NOT EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'U' AND schemas.[name] = 'dbo' AND objects.[name] = 'Queue')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The table Queue is missing. Contact your Ascent Technology Consultant. https://www.ascent.tech ', 16, 1
  END

  IF @DatabasesInParallel = 'Y' AND NOT EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'U' AND schemas.[name] = 'dbo' AND objects.[name] = 'QueueDatabase')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The table QueueDatabase is missing. Contact your Ascent Technology Consultant. https://www.ascent.tech ', 16, 1
  END

  IF @@TRANCOUNT <> 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The transaction count is not 0.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------
  --// Select databases                                                                           //--
  ----------------------------------------------------------------------------------------------------

  SET @Databases = REPLACE(@Databases, CHAR(10), '')
  SET @Databases = REPLACE(@Databases, CHAR(13), '')

  WHILE CHARINDEX(@StringDelimiter + ' ', @Databases) > 0 SET @Databases = REPLACE(@Databases, @StringDelimiter + ' ', @StringDelimiter)
  WHILE CHARINDEX(' ' + @StringDelimiter, @Databases) > 0 SET @Databases = REPLACE(@Databases, ' ' + @StringDelimiter, @StringDelimiter)

  SET @Databases = LTRIM(RTRIM(@Databases));

  WITH Databases1 (StartPosition, EndPosition, DatabaseItem) AS
  (
  SELECT 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Databases, 1), 0), LEN(@Databases) + 1) AS EndPosition,
         SUBSTRING(@Databases, 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Databases, 1), 0), LEN(@Databases) + 1) - 1) AS DatabaseItem
  WHERE @Databases IS NOT NULL
  UNION ALL
  SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Databases, EndPosition + 1), 0), LEN(@Databases) + 1) AS EndPosition,
         SUBSTRING(@Databases, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Databases, EndPosition + 1), 0), LEN(@Databases) + 1) - EndPosition - 1) AS DatabaseItem
  FROM Databases1
  WHERE EndPosition < LEN(@Databases) + 1
  ),
  Databases2 (DatabaseItem, StartPosition, Selected) AS
  (
  SELECT CASE WHEN DatabaseItem LIKE '-%' THEN RIGHT(DatabaseItem,LEN(DatabaseItem) - 1) ELSE DatabaseItem END AS DatabaseItem,
         StartPosition,
         CASE WHEN DatabaseItem LIKE '-%' THEN 0 ELSE 1 END AS Selected
  FROM Databases1
  ),
  Databases3 (DatabaseItem, DatabaseType, AvailabilityGroup, StartPosition, Selected) AS
  (
  SELECT CASE WHEN DatabaseItem IN('ALL_DATABASES','SYSTEM_DATABASES','USER_DATABASES','AVAILABILITY_GROUP_DATABASES') THEN '%' ELSE DatabaseItem END AS DatabaseItem,
         CASE WHEN DatabaseItem = 'SYSTEM_DATABASES' THEN 'S' WHEN DatabaseItem = 'USER_DATABASES' THEN 'U' ELSE NULL END AS DatabaseType,
         CASE WHEN DatabaseItem = 'AVAILABILITY_GROUP_DATABASES' THEN 1 ELSE NULL END AvailabilityGroup,
         StartPosition,
         Selected
  FROM Databases2
  ),
  Databases4 (DatabaseName, DatabaseType, AvailabilityGroup, StartPosition, Selected) AS
  (
  SELECT CASE WHEN LEFT(DatabaseItem,1) = '[' AND RIGHT(DatabaseItem,1) = ']' THEN PARSENAME(DatabaseItem,1) ELSE DatabaseItem END AS DatabaseItem,
         DatabaseType,
         AvailabilityGroup,
         StartPosition,
         Selected
  FROM Databases3
  )
  INSERT INTO @SelectedDatabases (DatabaseName, DatabaseType, AvailabilityGroup, StartPosition, Selected)
  SELECT DatabaseName,
         DatabaseType,
         AvailabilityGroup,
         StartPosition,
         Selected
  FROM Databases4
  OPTION (MAXRECURSION 0)

  IF @Version >= 11 AND SERVERPROPERTY('IsHadrEnabled') = 1
  BEGIN
    INSERT INTO @tmpAvailabilityGroups (AvailabilityGroupName, Selected)
    SELECT name AS AvailabilityGroupName,
           0 AS Selected
    FROM sys.availability_groups

    INSERT INTO @tmpDatabasesAvailabilityGroups (DatabaseName, AvailabilityGroupName)
    SELECT databases.name,
           availability_groups.name
    FROM sys.databases databases
    INNER JOIN sys.availability_replicas availability_replicas ON databases.replica_id = availability_replicas.replica_id
    INNER JOIN sys.availability_groups availability_groups ON availability_replicas.group_id = availability_groups.group_id
  END

  INSERT INTO @tmpDatabases (DatabaseName, DatabaseType, AvailabilityGroup, [Snapshot], [Order], Selected, Completed)
  SELECT [name] AS DatabaseName,
         CASE WHEN name IN('master','msdb','model') OR is_distributor = 1 THEN 'S' ELSE 'U' END AS DatabaseType,
         NULL AS AvailabilityGroup,
         CASE WHEN source_database_id IS NOT NULL THEN 1 ELSE 0 END AS [Snapshot],
         0 AS [Order],
         0 AS Selected,
         0 AS Completed
  FROM sys.databases
  ORDER BY [name] ASC

  UPDATE tmpDatabases
  SET AvailabilityGroup = CASE WHEN EXISTS (SELECT * FROM @tmpDatabasesAvailabilityGroups WHERE DatabaseName = tmpDatabases.DatabaseName) THEN 1 ELSE 0 END
  FROM @tmpDatabases tmpDatabases

  UPDATE tmpDatabases
  SET tmpDatabases.Selected = SelectedDatabases.Selected
  FROM @tmpDatabases tmpDatabases
  INNER JOIN @SelectedDatabases SelectedDatabases
  ON tmpDatabases.DatabaseName LIKE REPLACE(SelectedDatabases.DatabaseName,'_','[_]')
  AND (tmpDatabases.DatabaseType = SelectedDatabases.DatabaseType OR SelectedDatabases.DatabaseType IS NULL)
  AND (tmpDatabases.AvailabilityGroup = SelectedDatabases.AvailabilityGroup OR SelectedDatabases.AvailabilityGroup IS NULL)
  AND NOT ((tmpDatabases.DatabaseName = 'tempdb' OR tmpDatabases.[Snapshot] = 1) AND tmpDatabases.DatabaseName <> SelectedDatabases.DatabaseName)
  WHERE SelectedDatabases.Selected = 1

  UPDATE tmpDatabases
  SET tmpDatabases.Selected = SelectedDatabases.Selected
  FROM @tmpDatabases tmpDatabases
  INNER JOIN @SelectedDatabases SelectedDatabases
  ON tmpDatabases.DatabaseName LIKE REPLACE(SelectedDatabases.DatabaseName,'_','[_]')
  AND (tmpDatabases.DatabaseType = SelectedDatabases.DatabaseType OR SelectedDatabases.DatabaseType IS NULL)
  AND (tmpDatabases.AvailabilityGroup = SelectedDatabases.AvailabilityGroup OR SelectedDatabases.AvailabilityGroup IS NULL)
  AND NOT ((tmpDatabases.DatabaseName = 'tempdb' OR tmpDatabases.[Snapshot] = 1) AND tmpDatabases.DatabaseName <> SelectedDatabases.DatabaseName)
  WHERE SelectedDatabases.Selected = 0

  UPDATE tmpDatabases
  SET tmpDatabases.StartPosition = SelectedDatabases2.StartPosition
  FROM @tmpDatabases tmpDatabases
  INNER JOIN (SELECT tmpDatabases.DatabaseName, MIN(SelectedDatabases.StartPosition) AS StartPosition
              FROM @tmpDatabases tmpDatabases
              INNER JOIN @SelectedDatabases SelectedDatabases
              ON tmpDatabases.DatabaseName LIKE REPLACE(SelectedDatabases.DatabaseName,'_','[_]')
              AND (tmpDatabases.DatabaseType = SelectedDatabases.DatabaseType OR SelectedDatabases.DatabaseType IS NULL)
              AND (tmpDatabases.AvailabilityGroup = SelectedDatabases.AvailabilityGroup OR SelectedDatabases.AvailabilityGroup IS NULL)
              WHERE SelectedDatabases.Selected = 1
              GROUP BY tmpDatabases.DatabaseName) SelectedDatabases2
  ON tmpDatabases.DatabaseName = SelectedDatabases2.DatabaseName

  IF @Databases IS NOT NULL AND (NOT EXISTS(SELECT * FROM @SelectedDatabases) OR EXISTS(SELECT * FROM @SelectedDatabases WHERE DatabaseName IS NULL OR DatabaseName = ''))
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Databases is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------
  --// Select availability groups                                                                 //--
  ----------------------------------------------------------------------------------------------------

  IF @AvailabilityGroups IS NOT NULL AND @Version >= 11 AND SERVERPROPERTY('IsHadrEnabled') = 1
  BEGIN

    SET @AvailabilityGroups = REPLACE(@AvailabilityGroups, CHAR(10), '')
    SET @AvailabilityGroups = REPLACE(@AvailabilityGroups, CHAR(13), '')

    WHILE CHARINDEX(@StringDelimiter + ' ', @AvailabilityGroups) > 0 SET @AvailabilityGroups = REPLACE(@AvailabilityGroups, @StringDelimiter + ' ', @StringDelimiter)
    WHILE CHARINDEX(' ' + @StringDelimiter, @AvailabilityGroups) > 0 SET @AvailabilityGroups = REPLACE(@AvailabilityGroups, ' ' + @StringDelimiter, @StringDelimiter)

    SET @AvailabilityGroups = LTRIM(RTRIM(@AvailabilityGroups));

    WITH AvailabilityGroups1 (StartPosition, EndPosition, AvailabilityGroupItem) AS
    (
    SELECT 1 AS StartPosition,
           ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @AvailabilityGroups, 1), 0), LEN(@AvailabilityGroups) + 1) AS EndPosition,
           SUBSTRING(@AvailabilityGroups, 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @AvailabilityGroups, 1), 0), LEN(@AvailabilityGroups) + 1) - 1) AS AvailabilityGroupItem
    WHERE @AvailabilityGroups IS NOT NULL
    UNION ALL
    SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
           ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @AvailabilityGroups, EndPosition + 1), 0), LEN(@AvailabilityGroups) + 1) AS EndPosition,
           SUBSTRING(@AvailabilityGroups, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @AvailabilityGroups, EndPosition + 1), 0), LEN(@AvailabilityGroups) + 1) - EndPosition - 1) AS AvailabilityGroupItem
    FROM AvailabilityGroups1
    WHERE EndPosition < LEN(@AvailabilityGroups) + 1
    ),
    AvailabilityGroups2 (AvailabilityGroupItem, StartPosition, Selected) AS
    (
    SELECT CASE WHEN AvailabilityGroupItem LIKE '-%' THEN RIGHT(AvailabilityGroupItem,LEN(AvailabilityGroupItem) - 1) ELSE AvailabilityGroupItem END AS AvailabilityGroupItem,
           StartPosition,
           CASE WHEN AvailabilityGroupItem LIKE '-%' THEN 0 ELSE 1 END AS Selected
    FROM AvailabilityGroups1
    ),
    AvailabilityGroups3 (AvailabilityGroupItem, StartPosition, Selected) AS
    (
    SELECT CASE WHEN AvailabilityGroupItem = 'ALL_AVAILABILITY_GROUPS' THEN '%' ELSE AvailabilityGroupItem END AS AvailabilityGroupItem,
           StartPosition,
           Selected
    FROM AvailabilityGroups2
    ),
    AvailabilityGroups4 (AvailabilityGroupName, StartPosition, Selected) AS
    (
    SELECT CASE WHEN LEFT(AvailabilityGroupItem,1) = '[' AND RIGHT(AvailabilityGroupItem,1) = ']' THEN PARSENAME(AvailabilityGroupItem,1) ELSE AvailabilityGroupItem END AS AvailabilityGroupItem,
           StartPosition,
           Selected
    FROM AvailabilityGroups3
    )
    INSERT INTO @SelectedAvailabilityGroups (AvailabilityGroupName, StartPosition, Selected)
    SELECT AvailabilityGroupName, StartPosition, Selected
    FROM AvailabilityGroups4
    OPTION (MAXRECURSION 0)

    UPDATE tmpAvailabilityGroups
    SET tmpAvailabilityGroups.Selected = SelectedAvailabilityGroups.Selected
    FROM @tmpAvailabilityGroups tmpAvailabilityGroups
    INNER JOIN @SelectedAvailabilityGroups SelectedAvailabilityGroups
    ON tmpAvailabilityGroups.AvailabilityGroupName LIKE REPLACE(SelectedAvailabilityGroups.AvailabilityGroupName,'_','[_]')
    WHERE SelectedAvailabilityGroups.Selected = 1

    UPDATE tmpAvailabilityGroups
    SET tmpAvailabilityGroups.Selected = SelectedAvailabilityGroups.Selected
    FROM @tmpAvailabilityGroups tmpAvailabilityGroups
    INNER JOIN @SelectedAvailabilityGroups SelectedAvailabilityGroups
    ON tmpAvailabilityGroups.AvailabilityGroupName LIKE REPLACE(SelectedAvailabilityGroups.AvailabilityGroupName,'_','[_]')
    WHERE SelectedAvailabilityGroups.Selected = 0

    UPDATE tmpAvailabilityGroups
    SET tmpAvailabilityGroups.StartPosition = SelectedAvailabilityGroups2.StartPosition
    FROM @tmpAvailabilityGroups tmpAvailabilityGroups
    INNER JOIN (SELECT tmpAvailabilityGroups.AvailabilityGroupName, MIN(SelectedAvailabilityGroups.StartPosition) AS StartPosition
                FROM @tmpAvailabilityGroups tmpAvailabilityGroups
                INNER JOIN @SelectedAvailabilityGroups SelectedAvailabilityGroups
                ON tmpAvailabilityGroups.AvailabilityGroupName LIKE REPLACE(SelectedAvailabilityGroups.AvailabilityGroupName,'_','[_]')
                WHERE SelectedAvailabilityGroups.Selected = 1
                GROUP BY tmpAvailabilityGroups.AvailabilityGroupName) SelectedAvailabilityGroups2
    ON tmpAvailabilityGroups.AvailabilityGroupName = SelectedAvailabilityGroups2.AvailabilityGroupName

    UPDATE tmpDatabases
    SET tmpDatabases.StartPosition = tmpAvailabilityGroups.StartPosition,
        tmpDatabases.Selected = 1
    FROM @tmpDatabases tmpDatabases
    INNER JOIN @tmpDatabasesAvailabilityGroups tmpDatabasesAvailabilityGroups ON tmpDatabases.DatabaseName = tmpDatabasesAvailabilityGroups.DatabaseName
    INNER JOIN @tmpAvailabilityGroups tmpAvailabilityGroups ON tmpDatabasesAvailabilityGroups.AvailabilityGroupName = tmpAvailabilityGroups.AvailabilityGroupName
    WHERE tmpAvailabilityGroups.Selected = 1

  END

  IF @AvailabilityGroups IS NOT NULL AND (NOT EXISTS(SELECT * FROM @SelectedAvailabilityGroups) OR EXISTS(SELECT * FROM @SelectedAvailabilityGroups WHERE AvailabilityGroupName IS NULL OR AvailabilityGroupName = '') OR @Version < 11 OR SERVERPROPERTY('IsHadrEnabled') = 0)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @AvailabilityGroups is not supported.', 16, 1
  END

  IF (@Databases IS NULL AND @AvailabilityGroups IS NULL)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'You need to specify one of the parameters @Databases and @AvailabilityGroups.', 16, 2
  END

  IF (@Databases IS NOT NULL AND @AvailabilityGroups IS NOT NULL)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'You can only specify one of the parameters @Databases and @AvailabilityGroups.', 16, 3
  END

  ----------------------------------------------------------------------------------------------------
  --// Select filegroups                                                                          //--
  ----------------------------------------------------------------------------------------------------

  SET @FileGroups = REPLACE(@FileGroups, CHAR(10), '')
  SET @FileGroups = REPLACE(@FileGroups, CHAR(13), '')

  WHILE CHARINDEX(@StringDelimiter + ' ', @FileGroups) > 0 SET @FileGroups = REPLACE(@FileGroups, @StringDelimiter + ' ', @StringDelimiter)
  WHILE CHARINDEX(' ' + @StringDelimiter, @FileGroups) > 0 SET @FileGroups = REPLACE(@FileGroups, ' ' + @StringDelimiter, @StringDelimiter)

  SET @FileGroups = LTRIM(RTRIM(@FileGroups));

  WITH FileGroups1 (StartPosition, EndPosition, FileGroupItem) AS
  (
  SELECT 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @FileGroups, 1), 0), LEN(@FileGroups) + 1) AS EndPosition,
         SUBSTRING(@FileGroups, 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @FileGroups, 1), 0), LEN(@FileGroups) + 1) - 1) AS FileGroupItem
  WHERE @FileGroups IS NOT NULL
  UNION ALL
  SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @FileGroups, EndPosition + 1), 0), LEN(@FileGroups) + 1) AS EndPosition,
         SUBSTRING(@FileGroups, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @FileGroups, EndPosition + 1), 0), LEN(@FileGroups) + 1) - EndPosition - 1) AS FileGroupItem
  FROM FileGroups1
  WHERE EndPosition < LEN(@FileGroups) + 1
  ),
  FileGroups2 (FileGroupItem, StartPosition, Selected) AS
  (
  SELECT CASE WHEN FileGroupItem LIKE '-%' THEN RIGHT(FileGroupItem,LEN(FileGroupItem) - 1) ELSE FileGroupItem END AS FileGroupItem,
         StartPosition,
         CASE WHEN FileGroupItem LIKE '-%' THEN 0 ELSE 1 END AS Selected
  FROM FileGroups1
  ),
  FileGroups3 (FileGroupItem, StartPosition, Selected) AS
  (
  SELECT CASE WHEN FileGroupItem = 'ALL_FILEGROUPS' THEN '%.%' ELSE FileGroupItem END AS FileGroupItem,
         StartPosition,
         Selected
  FROM FileGroups2
  ),
  FileGroups4 (DatabaseName, FileGroupName, StartPosition, Selected) AS
  (
  SELECT CASE WHEN PARSENAME(FileGroupItem,4) IS NULL AND PARSENAME(FileGroupItem,3) IS NULL THEN PARSENAME(FileGroupItem,2) ELSE NULL END AS DatabaseName,
         CASE WHEN PARSENAME(FileGroupItem,4) IS NULL AND PARSENAME(FileGroupItem,3) IS NULL THEN PARSENAME(FileGroupItem,1) ELSE NULL END AS FileGroupName,
         StartPosition,
         Selected
  FROM FileGroups3
  )
  INSERT INTO @SelectedFileGroups (DatabaseName, FileGroupName, StartPosition, Selected)
  SELECT DatabaseName, FileGroupName, StartPosition, Selected
  FROM FileGroups4
  OPTION (MAXRECURSION 0)

  ----------------------------------------------------------------------------------------------------
  --// Select objects                                                                             //--
  ----------------------------------------------------------------------------------------------------

  SET @Objects = REPLACE(@Objects, CHAR(10), '')
  SET @Objects = REPLACE(@Objects, CHAR(13), '')

  WHILE CHARINDEX(@StringDelimiter + ' ', @Objects) > 0 SET @Objects = REPLACE(@Objects, @StringDelimiter + ' ', @StringDelimiter)
  WHILE CHARINDEX(' ' + @StringDelimiter, @Objects) > 0 SET @Objects = REPLACE(@Objects, ' ' + @StringDelimiter, @StringDelimiter)

  SET @Objects = LTRIM(RTRIM(@Objects));

  WITH Objects1 (StartPosition, EndPosition, ObjectItem) AS
  (
  SELECT 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Objects, 1), 0), LEN(@Objects) + 1) AS EndPosition,
         SUBSTRING(@Objects, 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Objects, 1), 0), LEN(@Objects) + 1) - 1) AS ObjectItem
  WHERE @Objects IS NOT NULL
  UNION ALL
  SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Objects, EndPosition + 1), 0), LEN(@Objects) + 1) AS EndPosition,
         SUBSTRING(@Objects, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Objects, EndPosition + 1), 0), LEN(@Objects) + 1) - EndPosition - 1) AS ObjectItem
  FROM Objects1
  WHERE EndPosition < LEN(@Objects) + 1
  ),
  Objects2 (ObjectItem, StartPosition, Selected) AS
  (
  SELECT CASE WHEN ObjectItem LIKE '-%' THEN RIGHT(ObjectItem,LEN(ObjectItem) - 1) ELSE ObjectItem END AS ObjectItem,
          StartPosition,
         CASE WHEN ObjectItem LIKE '-%' THEN 0 ELSE 1 END AS Selected
  FROM Objects1
  ),
  Objects3 (ObjectItem, StartPosition, Selected) AS
  (
  SELECT CASE WHEN ObjectItem = 'ALL_OBJECTS' THEN '%.%.%' ELSE ObjectItem END AS ObjectItem,
         StartPosition,
         Selected
  FROM Objects2
  ),
  Objects4 (DatabaseName, SchemaName, ObjectName, StartPosition, Selected) AS
  (
  SELECT CASE WHEN PARSENAME(ObjectItem,4) IS NULL THEN PARSENAME(ObjectItem,3) ELSE NULL END AS DatabaseName,
         CASE WHEN PARSENAME(ObjectItem,4) IS NULL THEN PARSENAME(ObjectItem,2) ELSE NULL END AS SchemaName,
         CASE WHEN PARSENAME(ObjectItem,4) IS NULL THEN PARSENAME(ObjectItem,1) ELSE NULL END AS ObjectName,
         StartPosition,
         Selected
  FROM Objects3
  )
  INSERT INTO @SelectedObjects (DatabaseName, SchemaName, ObjectName, StartPosition, Selected)
  SELECT DatabaseName, SchemaName, ObjectName, StartPosition, Selected
  FROM Objects4
  OPTION (MAXRECURSION 0)

  ----------------------------------------------------------------------------------------------------
  --// Select check commands                                                                      //--
  ----------------------------------------------------------------------------------------------------

  SET @CheckCommands = REPLACE(@CheckCommands, @StringDelimiter + ' ', @StringDelimiter);

  WITH CheckCommands (StartPosition, EndPosition, CheckCommand) AS
  (
  SELECT 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @CheckCommands, 1), 0), LEN(@CheckCommands) + 1) AS EndPosition,
         SUBSTRING(@CheckCommands, 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @CheckCommands, 1), 0), LEN(@CheckCommands) + 1) - 1) AS CheckCommand
  WHERE @CheckCommands IS NOT NULL
  UNION ALL
  SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @CheckCommands, EndPosition + 1), 0), LEN(@CheckCommands) + 1) AS EndPosition,
         SUBSTRING(@CheckCommands, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @CheckCommands, EndPosition + 1), 0), LEN(@CheckCommands) + 1) - EndPosition - 1) AS CheckCommand
  FROM CheckCommands
  WHERE EndPosition < LEN(@CheckCommands) + 1
  )
  INSERT INTO @SelectedCheckCommands (CheckCommand)
  SELECT CheckCommand
  FROM CheckCommands
  OPTION (MAXRECURSION 0)

  ----------------------------------------------------------------------------------------------------
  --// Check input parameters                                                                     //--
  ----------------------------------------------------------------------------------------------------

  IF EXISTS (SELECT * FROM @SelectedCheckCommands WHERE CheckCommand NOT IN('CHECKDB','CHECKFILEGROUP','CHECKALLOC','CHECKTABLE','CHECKCATALOG'))
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @CheckCommands is not supported.', 16, 1
  END

  IF EXISTS (SELECT * FROM @SelectedCheckCommands GROUP BY CheckCommand HAVING COUNT(*) > 1)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @CheckCommands is not supported.', 16, 2
  END

  IF NOT EXISTS (SELECT * FROM @SelectedCheckCommands)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @CheckCommands is not supported.' , 16, 3
  END

  IF EXISTS (SELECT * FROM @SelectedCheckCommands WHERE CheckCommand IN('CHECKDB')) AND EXISTS (SELECT CheckCommand FROM @SelectedCheckCommands WHERE CheckCommand IN('CHECKFILEGROUP','CHECKALLOC','CHECKTABLE','CHECKCATALOG'))
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @CheckCommands is not supported.', 16, 4
  END

  IF EXISTS (SELECT * FROM @SelectedCheckCommands WHERE CheckCommand IN('CHECKFILEGROUP')) AND EXISTS (SELECT CheckCommand FROM @SelectedCheckCommands WHERE CheckCommand IN('CHECKALLOC','CHECKTABLE'))
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @CheckCommands is not supported.', 16, 5
  END

  ----------------------------------------------------------------------------------------------------

  IF @PhysicalOnly NOT IN ('Y','N') OR @PhysicalOnly IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @PhysicalOnly is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @DataPurity NOT IN ('Y','N') OR @DataPurity IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @DataPurity is not supported.', 16, 1
  END

  IF @PhysicalOnly = 'Y' AND @DataPurity = 'Y'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The parameters @PhysicalOnly and @DataPurity cannot be used together.', 16, 2
  END

  ----------------------------------------------------------------------------------------------------

  IF @NoIndex NOT IN ('Y','N') OR @NoIndex IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @NoIndex is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @ExtendedLogicalChecks NOT IN ('Y','N') OR @ExtendedLogicalChecks IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @ExtendedLogicalChecks is not supported.', 16, 1
  END

  IF @PhysicalOnly = 'Y' AND @ExtendedLogicalChecks = 'Y'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The parameters @PhysicalOnly and @ExtendedLogicalChecks cannot be used together.', 16, 2
  END

  ----------------------------------------------------------------------------------------------------

  IF @TabLock NOT IN ('Y','N') OR @TabLock IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @TabLock is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF EXISTS(SELECT * FROM @SelectedFileGroups WHERE DatabaseName IS NULL OR FileGroupName IS NULL)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @FileGroups is not supported.', 16, 1
  END

  IF @FileGroups IS NOT NULL AND NOT EXISTS(SELECT * FROM @SelectedFileGroups)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @FileGroups is not supported.', 16, 2
  END

  IF @FileGroups IS NOT NULL AND NOT EXISTS (SELECT * FROM @SelectedCheckCommands WHERE CheckCommand = 'CHECKFILEGROUP')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @FileGroups is not supported.', 16, 3
  END

  ----------------------------------------------------------------------------------------------------

  IF EXISTS(SELECT * FROM @SelectedObjects WHERE DatabaseName IS NULL OR SchemaName IS NULL OR ObjectName IS NULL)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Objects is not supported.', 16, 1
  END

  IF (@Objects IS NOT NULL AND NOT EXISTS(SELECT * FROM @SelectedObjects))
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Objects is not supported.', 16, 2
  END

  IF (@Objects IS NOT NULL AND NOT EXISTS (SELECT * FROM @SelectedCheckCommands WHERE CheckCommand = 'CHECKTABLE'))
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Objects is not supported.', 16, 3
  END

  ----------------------------------------------------------------------------------------------------

  IF @MaxDOP < 0 OR @MaxDOP > 64
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @MaxDOP is not supported.', 16, 1
  END

  IF @MaxDOP IS NOT NULL AND NOT (@Version >= 12.050000 OR SERVERPROPERTY('EngineEdition') IN (5, 8))
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @MaxDOP is not supported. MAXDOP is not available in this version of SQL Server.', 16, 2
  END

  ----------------------------------------------------------------------------------------------------

  IF @AvailabilityGroupReplicas NOT IN('ALL','PRIMARY','SECONDARY','PREFERRED_BACKUP_REPLICA') OR @AvailabilityGroupReplicas IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @AvailabilityGroupReplicas is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @Updateability NOT IN('READ_ONLY','READ_WRITE','ALL') OR @Updateability IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Updateability is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @TimeLimit < 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @TimeLimit is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @LockTimeout < 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @LockTimeout is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @LockMessageSeverity NOT IN(10, 16)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @LockMessageSeverity is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @StringDelimiter IS NULL OR LEN(@StringDelimiter) > 1
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @StringDelimiter is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @DatabaseOrder NOT IN('DATABASE_NAME_ASC','DATABASE_NAME_DESC','DATABASE_SIZE_ASC','DATABASE_SIZE_DESC','DATABASE_LAST_GOOD_CHECK_ASC','DATABASE_LAST_GOOD_CHECK_DESC','REPLICA_LAST_GOOD_CHECK_ASC','REPLICA_LAST_GOOD_CHECK_DESC')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @DatabaseOrder is not supported.', 16, 1
  END

  IF @DatabaseOrder IN('DATABASE_LAST_GOOD_CHECK_ASC','DATABASE_LAST_GOOD_CHECK_DESC') AND NOT ((@Version >= 12.06024 AND @Version < 13) OR (@Version >= 13.05026 AND @Version < 14) OR @Version >= 14.0302916)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @DatabaseOrder is not supported. DATABASEPROPERTYEX(''DatabaseName'', ''LastGoodCheckDbTime'') is not available in this version of SQL Server.', 16, 2
  END

  IF @DatabaseOrder IN('REPLICA_LAST_GOOD_CHECK_ASC','REPLICA_LAST_GOOD_CHECK_DESC') AND @LogToTable = 'N'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @DatabaseOrder is not supported. You need to provide the parameter @LogToTable = ''Y''.', 16, 3
  END

  IF @DatabaseOrder IN('DATABASE_LAST_GOOD_CHECK_ASC','DATABASE_LAST_GOOD_CHECK_DESC','REPLICA_LAST_GOOD_CHECK_ASC','REPLICA_LAST_GOOD_CHECK_DESC') AND @CheckCommands <> 'CHECKDB'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @DatabaseOrder is not supported. You need to provide the parameter @CheckCommands = ''CHECKDB''.', 16, 4
  END

  IF @DatabaseOrder IS NOT NULL AND SERVERPROPERTY('EngineEdition') = 5
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @DatabaseOrder is not supported. This parameter is not supported in Azure SQL Database.', 16, 5
  END

  ----------------------------------------------------------------------------------------------------

  IF @DatabasesInParallel NOT IN('Y','N') OR @DatabasesInParallel IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @DatabasesInParallel is not supported.', 16, 1
  END

  IF @DatabasesInParallel = 'Y' AND SERVERPROPERTY('EngineEdition') = 5
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @DatabasesInParallel is not supported. This parameter is not supported in Azure SQL Database.', 16, 2
  END

  ----------------------------------------------------------------------------------------------------

  IF @LogToTable NOT IN('Y','N') OR @LogToTable IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @LogToTable is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @Execute NOT IN('Y','N') OR @Execute IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Execute is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF EXISTS(SELECT * FROM @Errors)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The documentation is available from Ascent Technology. https://www.ascent.tech', 16, 1
  END

  ----------------------------------------------------------------------------------------------------
  --// Check that selected databases and availability groups exist                                //--
  ----------------------------------------------------------------------------------------------------

  SET @ErrorMessage = ''
  SELECT @ErrorMessage = @ErrorMessage + QUOTENAME(DatabaseName) + ', '
  FROM @SelectedDatabases
  WHERE DatabaseName NOT LIKE '%[%]%'
  AND DatabaseName NOT IN (SELECT DatabaseName FROM @tmpDatabases)
  IF @@ROWCOUNT > 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The following databases in the @Databases parameter do not exist: ' + LEFT(@ErrorMessage,LEN(@ErrorMessage)-1) + '.', 10, 1
  END

  SET @ErrorMessage = ''
  SELECT @ErrorMessage = @ErrorMessage + QUOTENAME(DatabaseName) + ', '
  FROM @SelectedFileGroups
  WHERE DatabaseName NOT LIKE '%[%]%'
  AND DatabaseName NOT IN (SELECT DatabaseName FROM @tmpDatabases)
  IF @@ROWCOUNT > 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The following databases in the @FileGroups parameter do not exist: ' + LEFT(@ErrorMessage,LEN(@ErrorMessage)-1) + '.', 10, 1
  END

  SET @ErrorMessage = ''
  SELECT @ErrorMessage = @ErrorMessage + QUOTENAME(DatabaseName) + ', '
  FROM @SelectedObjects
  WHERE DatabaseName NOT LIKE '%[%]%'
  AND DatabaseName NOT IN (SELECT DatabaseName FROM @tmpDatabases)
  IF @@ROWCOUNT > 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The following databases in the @Objects parameter do not exist: ' + LEFT(@ErrorMessage,LEN(@ErrorMessage)-1) + '.', 10, 1
  END

  SET @ErrorMessage = ''
  SELECT @ErrorMessage = @ErrorMessage + QUOTENAME(AvailabilityGroupName) + ', '
  FROM @SelectedAvailabilityGroups
  WHERE AvailabilityGroupName NOT LIKE '%[%]%'
  AND AvailabilityGroupName NOT IN (SELECT AvailabilityGroupName FROM @tmpAvailabilityGroups)
  IF @@ROWCOUNT > 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The following availability groups do not exist: ' + LEFT(@ErrorMessage,LEN(@ErrorMessage)-1) + '.', 10, 1
  END

  SET @ErrorMessage = ''
  SELECT @ErrorMessage = @ErrorMessage + QUOTENAME(DatabaseName) + ', '
  FROM @SelectedFileGroups
  WHERE DatabaseName NOT LIKE '%[%]%'
  AND DatabaseName IN (SELECT DatabaseName FROM @tmpDatabases)
  AND DatabaseName NOT IN (SELECT DatabaseName FROM @tmpDatabases WHERE Selected = 1)
  IF @@ROWCOUNT > 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The following databases have been selected in the @FileGroups parameter, but not in the @Databases or @AvailabilityGroups parameters: ' + LEFT(@ErrorMessage,LEN(@ErrorMessage)-1) + '.', 10, 1
  END

  SET @ErrorMessage = ''
  SELECT @ErrorMessage = @ErrorMessage + QUOTENAME(DatabaseName) + ', '
  FROM @SelectedObjects
  WHERE DatabaseName NOT LIKE '%[%]%'
  AND DatabaseName IN (SELECT DatabaseName FROM @tmpDatabases)
  AND DatabaseName NOT IN (SELECT DatabaseName FROM @tmpDatabases WHERE Selected = 1)
  IF @@ROWCOUNT > 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The following databases have been selected in the @Objects parameter, but not in the @Databases or @AvailabilityGroups parameters: ' + LEFT(@ErrorMessage,LEN(@ErrorMessage)-1) + '.', 10, 1
  END

  ----------------------------------------------------------------------------------------------------
  --// Check @@SERVERNAME                                                                         //--
  ----------------------------------------------------------------------------------------------------

  IF @@SERVERNAME <> CAST(SERVERPROPERTY('ServerName') AS nvarchar(max)) AND SERVERPROPERTY('IsHadrEnabled') = 1
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The @@SERVERNAME does not match SERVERPROPERTY(''ServerName''). See ' + CASE WHEN SERVERPROPERTY('IsClustered') = 0 THEN 'https://docs.microsoft.com/en-us/sql/database-engine/install-windows/rename-a-computer-that-hosts-a-stand-alone-instance-of-sql-server' WHEN SERVERPROPERTY('IsClustered') = 1 THEN 'https://docs.microsoft.com/en-us/sql/sql-server/failover-clusters/install/rename-a-sql-server-failover-cluster-instance' END + '.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------
  --// Raise errors                                                                               //--
  ----------------------------------------------------------------------------------------------------

  DECLARE ErrorCursor CURSOR FAST_FORWARD FOR SELECT [Message], Severity, [State] FROM @Errors ORDER BY [ID] ASC

  OPEN ErrorCursor

  FETCH ErrorCursor INTO @CurrentMessage, @CurrentSeverity, @CurrentState

  WHILE @@FETCH_STATUS = 0
  BEGIN
    RAISERROR('%s', @CurrentSeverity, @CurrentState, @CurrentMessage) WITH NOWAIT
    RAISERROR(@EmptyLine, 10, 1) WITH NOWAIT

    FETCH NEXT FROM ErrorCursor INTO @CurrentMessage, @CurrentSeverity, @CurrentState
  END

  CLOSE ErrorCursor

  DEALLOCATE ErrorCursor

  IF EXISTS (SELECT * FROM @Errors WHERE Severity >= 16)
  BEGIN
    SET @ReturnCode = 50000
    GOTO Logging
  END

  ----------------------------------------------------------------------------------------------------
  --// Update database order                                                                      //--
  ----------------------------------------------------------------------------------------------------

  IF @DatabaseOrder IN('DATABASE_SIZE_ASC','DATABASE_SIZE_DESC')
  BEGIN
    UPDATE tmpDatabases
    SET DatabaseSize = (SELECT SUM(CAST(size AS bigint)) FROM sys.master_files WHERE [type] = 0 AND database_id = DB_ID(tmpDatabases.DatabaseName))
    FROM @tmpDatabases tmpDatabases
  END

  IF @DatabaseOrder IN('DATABASE_LAST_GOOD_CHECK_ASC','DATABASE_LAST_GOOD_CHECK_DESC')
  BEGIN
    UPDATE tmpDatabases
    SET LastGoodCheckDbTime = NULLIF(CAST(DATABASEPROPERTYEX (DatabaseName,'LastGoodCheckDbTime') AS datetime2),'1900-01-01 00:00:00.000')
    FROM @tmpDatabases tmpDatabases
  END

  IF @DatabaseOrder IN('REPLICA_LAST_GOOD_CHECK_ASC','REPLICA_LAST_GOOD_CHECK_DESC')
  BEGIN
    UPDATE tmpDatabases
    SET LastCommandTime = MaxStartTime
    FROM @tmpDatabases tmpDatabases
    INNER JOIN (SELECT DatabaseName, MAX(StartTime) AS MaxStartTime
                FROM dbo.CommandLog
                WHERE CommandType = 'DBCC_CHECKDB'
                AND ErrorNumber = 0
                GROUP BY DatabaseName) CommandLog
    ON tmpDatabases.DatabaseName = CommandLog.DatabaseName COLLATE DATABASE_DEFAULT
  END

  IF @DatabaseOrder IS NULL
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY StartPosition ASC, DatabaseName ASC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'DATABASE_NAME_ASC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY DatabaseName ASC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'DATABASE_NAME_DESC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY DatabaseName DESC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'DATABASE_SIZE_ASC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY DatabaseSize ASC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'DATABASE_SIZE_DESC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY DatabaseSize DESC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'DATABASE_LAST_GOOD_CHECK_ASC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY LastGoodCheckDbTime ASC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'DATABASE_LAST_GOOD_CHECK_DESC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY LastGoodCheckDbTime DESC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'REPLICA_LAST_GOOD_CHECK_ASC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY LastCommandTime ASC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'REPLICA_LAST_GOOD_CHECK_DESC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY LastCommandTime DESC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END

  ----------------------------------------------------------------------------------------------------
  --// Update the queue                                                                           //--
  ----------------------------------------------------------------------------------------------------

  IF @DatabasesInParallel = 'Y'
  BEGIN

    BEGIN TRY

      SELECT @QueueID = QueueID
      FROM dbo.[Queue]
      WHERE SchemaName = @SchemaName
      AND ObjectName = @ObjectName
      AND [Parameters] = @Parameters

      IF @QueueID IS NULL
      BEGIN
        BEGIN TRANSACTION

        SELECT @QueueID = QueueID
        FROM dbo.[Queue] WITH (UPDLOCK, HOLDLOCK)
        WHERE SchemaName = @SchemaName
        AND ObjectName = @ObjectName
        AND [Parameters] = @Parameters

        IF @QueueID IS NULL
        BEGIN
          INSERT INTO dbo.[Queue] (SchemaName, ObjectName, [Parameters])
          SELECT @SchemaName, @ObjectName, @Parameters

          SET @QueueID = SCOPE_IDENTITY()
        END

        COMMIT TRANSACTION
      END

      BEGIN TRANSACTION

      UPDATE [Queue]
      SET QueueStartTime = SYSDATETIME(),
          SessionID = @@SPID,
          RequestID = (SELECT request_id FROM sys.dm_exec_requests WHERE session_id = @@SPID),
          RequestStartTime = (SELECT start_time FROM sys.dm_exec_requests WHERE session_id = @@SPID)
      FROM dbo.[Queue] [Queue]
      WHERE QueueID = @QueueID
      AND NOT EXISTS (SELECT *
                      FROM sys.dm_exec_requests
                      WHERE session_id = [Queue].SessionID
                      AND request_id = [Queue].RequestID
                      AND start_time = [Queue].RequestStartTime)
      AND NOT EXISTS (SELECT *
                      FROM dbo.QueueDatabase QueueDatabase
                      INNER JOIN sys.dm_exec_requests ON QueueDatabase.SessionID = session_id AND QueueDatabase.RequestID = request_id AND QueueDatabase.RequestStartTime = start_time
                      WHERE QueueDatabase.QueueID = @QueueID)

      IF @@ROWCOUNT = 1
      BEGIN
        INSERT INTO dbo.QueueDatabase (QueueID, DatabaseName)
        SELECT @QueueID AS QueueID,
               DatabaseName
        FROM @tmpDatabases tmpDatabases
        WHERE Selected = 1
        AND NOT EXISTS (SELECT * FROM dbo.QueueDatabase WHERE DatabaseName = tmpDatabases.DatabaseName AND QueueID = @QueueID)

        DELETE QueueDatabase
        FROM dbo.QueueDatabase QueueDatabase
        WHERE QueueID = @QueueID
        AND NOT EXISTS (SELECT * FROM @tmpDatabases tmpDatabases WHERE DatabaseName = QueueDatabase.DatabaseName AND Selected = 1)

        UPDATE QueueDatabase
        SET DatabaseOrder = tmpDatabases.[Order]
        FROM dbo.QueueDatabase QueueDatabase
        INNER JOIN @tmpDatabases tmpDatabases ON QueueDatabase.DatabaseName = tmpDatabases.DatabaseName
        WHERE QueueID = @QueueID
      END

      COMMIT TRANSACTION

      SELECT @QueueStartTime = QueueStartTime
      FROM dbo.[Queue]
      WHERE QueueID = @QueueID

    END TRY

    BEGIN CATCH
      IF XACT_STATE() <> 0
      BEGIN
        ROLLBACK TRANSACTION
      END
      SET @ErrorMessage = 'Msg ' + CAST(ERROR_NUMBER() AS nvarchar) + ', ' + ISNULL(ERROR_MESSAGE(),'')
      RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
      RAISERROR(@EmptyLine,10,1) WITH NOWAIT
      SET @ReturnCode = ERROR_NUMBER()
      GOTO Logging
    END CATCH

  END

  ----------------------------------------------------------------------------------------------------
  --// Execute commands                                                                           //--
  ----------------------------------------------------------------------------------------------------

  WHILE (1 = 1)
  BEGIN

    IF @DatabasesInParallel = 'Y'
    BEGIN
      UPDATE QueueDatabase
      SET DatabaseStartTime = NULL,
          SessionID = NULL,
          RequestID = NULL,
          RequestStartTime = NULL
      FROM dbo.QueueDatabase QueueDatabase
      WHERE QueueID = @QueueID
      AND DatabaseStartTime IS NOT NULL
      AND DatabaseEndTime IS NULL
      AND NOT EXISTS (SELECT * FROM sys.dm_exec_requests WHERE session_id = QueueDatabase.SessionID AND request_id = QueueDatabase.RequestID AND start_time = QueueDatabase.RequestStartTime)

      UPDATE QueueDatabase
      SET DatabaseStartTime = SYSDATETIME(),
          DatabaseEndTime = NULL,
          SessionID = @@SPID,
          RequestID = (SELECT request_id FROM sys.dm_exec_requests WHERE session_id = @@SPID),
          RequestStartTime = (SELECT start_time FROM sys.dm_exec_requests WHERE session_id = @@SPID),
          @CurrentDatabaseName = DatabaseName
      FROM (SELECT TOP 1 DatabaseStartTime,
                         DatabaseEndTime,
                         SessionID,
                         RequestID,
                         RequestStartTime,
                         DatabaseName
            FROM dbo.QueueDatabase
            WHERE QueueID = @QueueID
            AND (DatabaseStartTime < @QueueStartTime OR DatabaseStartTime IS NULL)
            AND NOT (DatabaseStartTime IS NOT NULL AND DatabaseEndTime IS NULL)
            ORDER BY DatabaseOrder ASC
            ) QueueDatabase
    END
    ELSE
    BEGIN
      SELECT TOP 1 @CurrentDBID = ID,
                   @CurrentDatabaseName = DatabaseName
      FROM @tmpDatabases
      WHERE Selected = 1
      AND Completed = 0
      ORDER BY [Order] ASC
    END

    IF @@ROWCOUNT = 0
    BEGIN
     BREAK
    END

    SET @CurrentDatabase_sp_executesql = QUOTENAME(@CurrentDatabaseName) + '.sys.sp_executesql'

    BEGIN
      SET @DatabaseMessage = 'Date and time: ' + CONVERT(nvarchar,SYSDATETIME(),120)
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Database: ' + QUOTENAME(@CurrentDatabaseName)
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
    END

    SELECT @CurrentUserAccess = user_access_desc,
           @CurrentIsReadOnly = is_read_only,
           @CurrentDatabaseState = state_desc,
           @CurrentInStandby = is_in_standby,
           @CurrentRecoveryModel = recovery_model_desc
    FROM sys.databases
    WHERE [name] = @CurrentDatabaseName

    BEGIN
      SET @DatabaseMessage = 'State: ' + @CurrentDatabaseState
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Standby: ' + CASE WHEN @CurrentInStandby = 1 THEN 'Yes' ELSE 'No' END
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Updateability: ' + CASE WHEN @CurrentIsReadOnly = 1 THEN 'READ_ONLY' WHEN  @CurrentIsReadOnly = 0 THEN 'READ_WRITE' END
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'User access: ' + @CurrentUserAccess
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Recovery model: ' + @CurrentRecoveryModel
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
    END

    IF @CurrentDatabaseState IN('ONLINE','EMERGENCY') AND SERVERPROPERTY('EngineEdition') <> 5
    BEGIN
      IF EXISTS (SELECT * FROM sys.database_recovery_status WHERE database_id = DB_ID(@CurrentDatabaseName) AND database_guid IS NOT NULL)
      BEGIN
        SET @CurrentIsDatabaseAccessible = 1
      END
      ELSE
      BEGIN
        SET @CurrentIsDatabaseAccessible = 0
      END
    END

    IF @Version >= 11 AND SERVERPROPERTY('IsHadrEnabled') = 1
    BEGIN
      SELECT @CurrentReplicaID = databases.replica_id
      FROM sys.databases databases
      INNER JOIN sys.availability_replicas availability_replicas ON databases.replica_id = availability_replicas.replica_id
      WHERE databases.[name] = @CurrentDatabaseName

      SELECT @CurrentAvailabilityGroupID = group_id,
             @CurrentSecondaryRoleAllowConnections = secondary_role_allow_connections_desc
      FROM sys.availability_replicas
      WHERE replica_id = @CurrentReplicaID

      SELECT @CurrentAvailabilityGroupRole = role_desc
      FROM sys.dm_hadr_availability_replica_states
      WHERE replica_id = @CurrentReplicaID

      SELECT @CurrentAvailabilityGroup = [name],
             @CurrentAvailabilityGroupBackupPreference = UPPER(automated_backup_preference_desc)
      FROM sys.availability_groups
      WHERE group_id = @CurrentAvailabilityGroupID
    END

    IF @Version >= 11 AND SERVERPROPERTY('IsHadrEnabled') = 1 AND @CurrentAvailabilityGroup IS NOT NULL AND @AvailabilityGroupReplicas = 'PREFERRED_BACKUP_REPLICA'
    BEGIN
      SELECT @CurrentIsPreferredBackupReplica = sys.fn_hadr_backup_is_preferred_replica(@CurrentDatabaseName)
    END

    IF SERVERPROPERTY('EngineEdition') <> 5
    BEGIN
      SELECT @CurrentDatabaseMirroringRole = UPPER(mirroring_role_desc)
      FROM sys.database_mirroring
      WHERE database_id = DB_ID(@CurrentDatabaseName)
    END

    IF @CurrentIsDatabaseAccessible IS NOT NULL
    BEGIN
      SET @DatabaseMessage = 'Is accessible: ' + CASE WHEN @CurrentIsDatabaseAccessible = 1 THEN 'Yes' ELSE 'No' END
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
    END

    IF @CurrentAvailabilityGroup IS NOT NULL
    BEGIN
      SET @DatabaseMessage =  'Availability group: ' + ISNULL(@CurrentAvailabilityGroup,'N/A')
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Availability group role: ' + ISNULL(@CurrentAvailabilityGroupRole,'N/A')
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      IF @CurrentAvailabilityGroupRole = 'SECONDARY'
      BEGIN
        SET @DatabaseMessage =  'Readable Secondary: ' + ISNULL(@CurrentSecondaryRoleAllowConnections,'N/A')
        RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
      END

      IF @AvailabilityGroupReplicas = 'PREFERRED_BACKUP_REPLICA'
      BEGIN
        SET @DatabaseMessage = 'Availability group backup preference: ' + ISNULL(@CurrentAvailabilityGroupBackupPreference,'N/A')
        RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

        SET @DatabaseMessage = 'Is preferred backup replica: ' + CASE WHEN @CurrentIsPreferredBackupReplica = 1 THEN 'Yes' WHEN @CurrentIsPreferredBackupReplica = 0 THEN 'No' ELSE 'N/A' END
        RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
      END
    END

    IF @CurrentDatabaseMirroringRole IS NOT NULL
    BEGIN
      SET @DatabaseMessage = 'Database mirroring role: ' + @CurrentDatabaseMirroringRole
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
    END

    RAISERROR(@EmptyLine,10,1) WITH NOWAIT

    IF @CurrentDatabaseState IN('ONLINE','EMERGENCY')
    AND NOT (@CurrentUserAccess = 'SINGLE_USER' AND @CurrentIsDatabaseAccessible = 0)
    AND (@CurrentAvailabilityGroupRole = 'PRIMARY' OR @CurrentAvailabilityGroupRole IS NULL OR SERVERPROPERTY('EngineEdition') = 3)
    AND ((@AvailabilityGroupReplicas = 'PRIMARY' AND @CurrentAvailabilityGroupRole = 'PRIMARY') OR (@AvailabilityGroupReplicas = 'SECONDARY' AND @CurrentAvailabilityGroupRole = 'SECONDARY') OR (@AvailabilityGroupReplicas = 'PREFERRED_BACKUP_REPLICA' AND @CurrentIsPreferredBackupReplica = 1) OR @AvailabilityGroupReplicas = 'ALL' OR @CurrentAvailabilityGroupRole IS NULL)
    AND NOT (@CurrentIsReadOnly = 1 AND @Updateability = 'READ_WRITE')
    AND NOT (@CurrentIsReadOnly = 0 AND @Updateability = 'READ_ONLY')
    BEGIN

      -- Check database
      IF EXISTS(SELECT * FROM @SelectedCheckCommands WHERE CheckCommand = 'CHECKDB') AND (SYSDATETIME() < DATEADD(SECOND,@TimeLimit,@StartTime) OR @TimeLimit IS NULL)
      BEGIN
        SET @CurrentDatabaseContext = CASE WHEN SERVERPROPERTY('EngineEdition') = 5 THEN @CurrentDatabaseName ELSE 'master' END

        SET @CurrentCommandType = 'DBCC_CHECKDB'

        SET @CurrentCommand = ''
        IF @LockTimeout IS NOT NULL SET @CurrentCommand = 'SET LOCK_TIMEOUT ' + CAST(@LockTimeout * 1000 AS nvarchar) + '; '
        SET @CurrentCommand += 'DBCC CHECKDB (' + QUOTENAME(@CurrentDatabaseName)
        IF @NoIndex = 'Y' SET @CurrentCommand += ', NOINDEX'
        SET @CurrentCommand += ') WITH NO_INFOMSGS, ALL_ERRORMSGS'
        IF @DataPurity = 'Y' SET @CurrentCommand += ', DATA_PURITY'
        IF @PhysicalOnly = 'Y' SET @CurrentCommand += ', PHYSICAL_ONLY'
        IF @ExtendedLogicalChecks = 'Y' SET @CurrentCommand += ', EXTENDED_LOGICAL_CHECKS'
        IF @TabLock = 'Y' SET @CurrentCommand += ', TABLOCK'
        IF @MaxDOP IS NOT NULL SET @CurrentCommand += ', MAXDOP = ' + CAST(@MaxDOP AS nvarchar)

        EXECUTE @CurrentCommandOutput = dbo.CommandExecute @DatabaseContext = @CurrentDatabaseContext, @Command = @CurrentCommand, @CommandType = @CurrentCommandType, @Mode = 1, @DatabaseName = @CurrentDatabaseName, @LogToTable = @LogToTable, @Execute = @Execute
        SET @Error = @@ERROR
        IF @Error <> 0 SET @CurrentCommandOutput = @Error
        IF @CurrentCommandOutput <> 0 SET @ReturnCode = @CurrentCommandOutput
      END

      -- Check filegroups
      IF EXISTS(SELECT * FROM @SelectedCheckCommands WHERE CheckCommand = 'CHECKFILEGROUP')
      AND (@CurrentAvailabilityGroupRole = 'PRIMARY' OR (@CurrentAvailabilityGroupRole = 'SECONDARY' AND @CurrentSecondaryRoleAllowConnections = 'ALL') OR @CurrentAvailabilityGroupRole IS NULL)
      AND (SYSDATETIME() < DATEADD(SECOND,@TimeLimit,@StartTime) OR @TimeLimit IS NULL)
      BEGIN
        SET @CurrentCommand = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; SELECT data_space_id AS FileGroupID, name AS FileGroupName, 0 AS [Order], 0 AS Selected, 0 AS Completed FROM sys.filegroups filegroups WHERE [type] <> ''FX'' ORDER BY CASE WHEN filegroups.name = ''PRIMARY'' THEN 1 ELSE 0 END DESC, filegroups.name ASC'

        INSERT INTO @tmpFileGroups (FileGroupID, FileGroupName, [Order], Selected, Completed)
        EXECUTE @CurrentDatabase_sp_executesql  @stmt = @CurrentCommand
        SET @Error = @@ERROR
        IF @Error <> 0 SET @ReturnCode = @Error

        IF @FileGroups IS NULL
        BEGIN
          UPDATE tmpFileGroups
          SET tmpFileGroups.Selected = 1
          FROM @tmpFileGroups tmpFileGroups
        END
        ELSE
        BEGIN
          UPDATE tmpFileGroups
          SET tmpFileGroups.Selected = SelectedFileGroups.Selected
          FROM @tmpFileGroups tmpFileGroups
          INNER JOIN @SelectedFileGroups SelectedFileGroups
          ON @CurrentDatabaseName LIKE REPLACE(SelectedFileGroups.DatabaseName,'_','[_]') AND tmpFileGroups.FileGroupName LIKE REPLACE(SelectedFileGroups.FileGroupName,'_','[_]')
          WHERE SelectedFileGroups.Selected = 1

          UPDATE tmpFileGroups
          SET tmpFileGroups.Selected = SelectedFileGroups.Selected
          FROM @tmpFileGroups tmpFileGroups
          INNER JOIN @SelectedFileGroups SelectedFileGroups
          ON @CurrentDatabaseName LIKE REPLACE(SelectedFileGroups.DatabaseName,'_','[_]') AND tmpFileGroups.FileGroupName LIKE REPLACE(SelectedFileGroups.FileGroupName,'_','[_]')
          WHERE SelectedFileGroups.Selected = 0

          UPDATE tmpFileGroups
          SET tmpFileGroups.StartPosition = SelectedFileGroups2.StartPosition
          FROM @tmpFileGroups tmpFileGroups
          INNER JOIN (SELECT tmpFileGroups.FileGroupName, MIN(SelectedFileGroups.StartPosition) AS StartPosition
                      FROM @tmpFileGroups tmpFileGroups
                      INNER JOIN @SelectedFileGroups SelectedFileGroups
                      ON @CurrentDatabaseName LIKE REPLACE(SelectedFileGroups.DatabaseName,'_','[_]') AND tmpFileGroups.FileGroupName LIKE REPLACE(SelectedFileGroups.FileGroupName,'_','[_]')
                      WHERE SelectedFileGroups.Selected = 1
                      GROUP BY tmpFileGroups.FileGroupName) SelectedFileGroups2
          ON tmpFileGroups.FileGroupName = SelectedFileGroups2.FileGroupName
        END;

        WITH tmpFileGroups AS (
        SELECT FileGroupName, [Order], ROW_NUMBER() OVER (ORDER BY StartPosition ASC, FileGroupName ASC) AS RowNumber
        FROM @tmpFileGroups tmpFileGroups
        WHERE Selected = 1
        )
        UPDATE tmpFileGroups
        SET [Order] = RowNumber

        SET @ErrorMessage = ''
        SELECT @ErrorMessage = @ErrorMessage + QUOTENAME(DatabaseName) + '.' + QUOTENAME(FileGroupName) + ', '
        FROM @SelectedFileGroups SelectedFileGroups
        WHERE DatabaseName = @CurrentDatabaseName
        AND FileGroupName NOT LIKE '%[%]%'
        AND NOT EXISTS (SELECT * FROM @tmpFileGroups WHERE FileGroupName = SelectedFileGroups.FileGroupName)
        IF @@ROWCOUNT > 0
        BEGIN
          SET @ErrorMessage = 'The following file groups do not exist: ' + LEFT(@ErrorMessage,LEN(@ErrorMessage)-1) + '.'
          RAISERROR('%s',10,1,@ErrorMessage) WITH NOWAIT
          SET @Error = @@ERROR
          RAISERROR(@EmptyLine,10,1) WITH NOWAIT
        END

        WHILE (SYSDATETIME() < DATEADD(SECOND,@TimeLimit,@StartTime) OR @TimeLimit IS NULL)
        BEGIN
          SELECT TOP 1 @CurrentFGID = ID,
                       @CurrentFileGroupID = FileGroupID,
                       @CurrentFileGroupName = FileGroupName
          FROM @tmpFileGroups
          WHERE Selected = 1
          AND Completed = 0
          ORDER BY [Order] ASC

          IF @@ROWCOUNT = 0
          BEGIN
            BREAK
          END

          -- Does the filegroup exist?
          SET @CurrentCommand = ''
          IF @LockTimeout IS NOT NULL SET @CurrentCommand = 'SET LOCK_TIMEOUT ' + CAST(@LockTimeout * 1000 AS nvarchar) + '; '
          SET @CurrentCommand += 'IF EXISTS(SELECT * FROM sys.filegroups filegroups WHERE [type] <> ''FX'' AND filegroups.data_space_id = @ParamFileGroupID AND filegroups.[name] = @ParamFileGroupName) BEGIN SET @ParamFileGroupExists = 1 END'

          BEGIN TRY
            EXECUTE @CurrentDatabase_sp_executesql @stmt = @CurrentCommand, @params = N'@ParamFileGroupID int, @ParamFileGroupName sysname, @ParamFileGroupExists bit OUTPUT', @ParamFileGroupID = @CurrentFileGroupID, @ParamFileGroupName = @CurrentFileGroupName, @ParamFileGroupExists = @CurrentFileGroupExists OUTPUT

            IF @CurrentFileGroupExists IS NULL SET @CurrentFileGroupExists = 0
          END TRY
          BEGIN CATCH
            SET @ErrorMessage = 'Msg ' + CAST(ERROR_NUMBER() AS nvarchar) + ', ' + ISNULL(ERROR_MESSAGE(),'') + CASE WHEN ERROR_NUMBER() = 1222 THEN ', ' + ' The file group ' + QUOTENAME(@CurrentFileGroupName) + ' in the database ' + QUOTENAME(@CurrentDatabaseName) + ' is locked. It could not be checked if the filegroup exists.' ELSE '' END
            SET @Severity = CASE WHEN ERROR_NUMBER() IN(1205,1222) THEN @LockMessageSeverity ELSE 16 END
            RAISERROR('%s',@Severity,1,@ErrorMessage) WITH NOWAIT
            RAISERROR(@EmptyLine,10,1) WITH NOWAIT

            IF NOT (ERROR_NUMBER() IN(1205,1222) AND @LockMessageSeverity = 10)
            BEGIN
              SET @ReturnCode = ERROR_NUMBER()
            END
          END CATCH

          IF @CurrentFileGroupExists = 1
          BEGIN
            SET @CurrentDatabaseContext = @CurrentDatabaseName

            SET @CurrentCommandType = 'DBCC_CHECKFILEGROUP'

            SET @CurrentCommand = ''
            IF @LockTimeout IS NOT NULL SET @CurrentCommand = 'SET LOCK_TIMEOUT ' + CAST(@LockTimeout * 1000 AS nvarchar) + '; '
            SET @CurrentCommand += 'DBCC CHECKFILEGROUP (' + QUOTENAME(@CurrentFileGroupName)
            IF @NoIndex = 'Y' SET @CurrentCommand += ', NOINDEX'
            SET @CurrentCommand += ') WITH NO_INFOMSGS, ALL_ERRORMSGS'
            IF @PhysicalOnly = 'Y' SET @CurrentCommand += ', PHYSICAL_ONLY'
            IF @TabLock = 'Y' SET @CurrentCommand += ', TABLOCK'
            IF @MaxDOP IS NOT NULL SET @CurrentCommand += ', MAXDOP = ' + CAST(@MaxDOP AS nvarchar)

            EXECUTE @CurrentCommandOutput = dbo.CommandExecute @DatabaseContext = @CurrentDatabaseContext, @Command = @CurrentCommand, @CommandType = @CurrentCommandType, @Mode = 1, @DatabaseName = @CurrentDatabaseName, @LogToTable = @LogToTable, @Execute = @Execute
            SET @Error = @@ERROR
            IF @Error <> 0 SET @CurrentCommandOutput = @Error
            IF @CurrentCommandOutput <> 0 SET @ReturnCode = @CurrentCommandOutput
          END

          UPDATE @tmpFileGroups
          SET Completed = 1
          WHERE Selected = 1
          AND Completed = 0
          AND ID = @CurrentFGID

          SET @CurrentFGID = NULL
          SET @CurrentFileGroupID = NULL
          SET @CurrentFileGroupName = NULL
          SET @CurrentFileGroupExists = NULL

          SET @CurrentDatabaseContext = NULL
          SET @CurrentCommand = NULL
          SET @CurrentCommandOutput = NULL
          SET @CurrentCommandType = NULL
        END
      END

      -- Check disk space allocation structures
      IF EXISTS(SELECT * FROM @SelectedCheckCommands WHERE CheckCommand = 'CHECKALLOC') AND (SYSDATETIME() < DATEADD(SECOND,@TimeLimit,@StartTime) OR @TimeLimit IS NULL)
      BEGIN
        SET @CurrentDatabaseContext = CASE WHEN SERVERPROPERTY('EngineEdition') = 5 THEN @CurrentDatabaseName ELSE 'master' END

        SET @CurrentCommandType = 'DBCC_CHECKALLOC'

        SET @CurrentCommand = ''
        IF @LockTimeout IS NOT NULL SET @CurrentCommand = 'SET LOCK_TIMEOUT ' + CAST(@LockTimeout * 1000 AS nvarchar) + '; '
        SET @CurrentCommand += 'DBCC CHECKALLOC (' + QUOTENAME(@CurrentDatabaseName)
        SET @CurrentCommand += ') WITH NO_INFOMSGS, ALL_ERRORMSGS'
        IF @TabLock = 'Y' SET @CurrentCommand += ', TABLOCK'

        EXECUTE @CurrentCommandOutput = dbo.CommandExecute @DatabaseContext = @CurrentDatabaseContext, @Command = @CurrentCommand, @CommandType = @CurrentCommandType, @Mode = 1, @DatabaseName = @CurrentDatabaseName, @LogToTable = @LogToTable, @Execute = @Execute
        SET @Error = @@ERROR
        IF @Error <> 0 SET @CurrentCommandOutput = @Error
        IF @CurrentCommandOutput <> 0 SET @ReturnCode = @CurrentCommandOutput
      END

      -- Check objects
      IF EXISTS(SELECT * FROM @SelectedCheckCommands WHERE CheckCommand = 'CHECKTABLE')
      AND (@CurrentAvailabilityGroupRole = 'PRIMARY' OR (@CurrentAvailabilityGroupRole = 'SECONDARY' AND @CurrentSecondaryRoleAllowConnections = 'ALL') OR @CurrentAvailabilityGroupRole IS NULL)
      AND (SYSDATETIME() < DATEADD(SECOND,@TimeLimit,@StartTime) OR @TimeLimit IS NULL)
      BEGIN
        SET @CurrentCommand = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; SELECT schemas.[schema_id] AS SchemaID, schemas.[name] AS SchemaName, objects.[object_id] AS ObjectID, objects.[name] AS ObjectName, RTRIM(objects.[type]) AS ObjectType, 0 AS [Order], 0 AS Selected, 0 AS Completed FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.schema_id = schemas.schema_id LEFT OUTER JOIN sys.tables tables ON objects.object_id = tables.object_id WHERE objects.[type] IN(''U'',''V'') AND EXISTS(SELECT * FROM sys.indexes indexes WHERE indexes.object_id = objects.object_id)' + CASE WHEN @Version >= 12 THEN ' AND (tables.is_memory_optimized = 0 OR is_memory_optimized IS NULL)' ELSE '' END + ' ORDER BY schemas.name ASC, objects.name ASC'

        INSERT INTO @tmpObjects (SchemaID, SchemaName, ObjectID, ObjectName, ObjectType, [Order], Selected, Completed)
        EXECUTE @CurrentDatabase_sp_executesql @stmt = @CurrentCommand
        SET @Error = @@ERROR
        IF @Error <> 0 SET @ReturnCode = @Error

        IF @Objects IS NULL
        BEGIN
          UPDATE tmpObjects
          SET tmpObjects.Selected = 1
          FROM @tmpObjects tmpObjects
        END
        ELSE
        BEGIN
          UPDATE tmpObjects
          SET tmpObjects.Selected = SelectedObjects.Selected
          FROM @tmpObjects tmpObjects
          INNER JOIN @SelectedObjects SelectedObjects
          ON @CurrentDatabaseName LIKE REPLACE(SelectedObjects.DatabaseName,'_','[_]') AND tmpObjects.SchemaName LIKE REPLACE(SelectedObjects.SchemaName,'_','[_]') AND tmpObjects.ObjectName LIKE REPLACE(SelectedObjects.ObjectName,'_','[_]')
          WHERE SelectedObjects.Selected = 1

          UPDATE tmpObjects
          SET tmpObjects.Selected = SelectedObjects.Selected
          FROM @tmpObjects tmpObjects
          INNER JOIN @SelectedObjects SelectedObjects
          ON @CurrentDatabaseName LIKE REPLACE(SelectedObjects.DatabaseName,'_','[_]') AND tmpObjects.SchemaName LIKE REPLACE(SelectedObjects.SchemaName,'_','[_]') AND tmpObjects.ObjectName LIKE REPLACE(SelectedObjects.ObjectName,'_','[_]')
          WHERE SelectedObjects.Selected = 0

          UPDATE tmpObjects
          SET tmpObjects.StartPosition = SelectedObjects2.StartPosition
          FROM @tmpObjects tmpObjects
          INNER JOIN (SELECT tmpObjects.SchemaName, tmpObjects.ObjectName, MIN(SelectedObjects.StartPosition) AS StartPosition
                      FROM @tmpObjects tmpObjects
                      INNER JOIN @SelectedObjects SelectedObjects
                      ON @CurrentDatabaseName LIKE REPLACE(SelectedObjects.DatabaseName,'_','[_]') AND tmpObjects.SchemaName LIKE REPLACE(SelectedObjects.SchemaName,'_','[_]') AND tmpObjects.ObjectName LIKE REPLACE(SelectedObjects.ObjectName,'_','[_]')
                      WHERE SelectedObjects.Selected = 1
                      GROUP BY tmpObjects.SchemaName, tmpObjects.ObjectName) SelectedObjects2
          ON tmpObjects.SchemaName = SelectedObjects2.SchemaName AND tmpObjects.ObjectName = SelectedObjects2.ObjectName
        END;

        WITH tmpObjects AS (
        SELECT SchemaName, ObjectName, [Order], ROW_NUMBER() OVER (ORDER BY StartPosition ASC, SchemaName ASC, ObjectName ASC) AS RowNumber
        FROM @tmpObjects tmpObjects
        WHERE Selected = 1
        )
        UPDATE tmpObjects
        SET [Order] = RowNumber

        SET @ErrorMessage = ''
        SELECT @ErrorMessage = @ErrorMessage + QUOTENAME(DatabaseName) + '.' + QUOTENAME(SchemaName) + '.' + QUOTENAME(ObjectName) + ', '
        FROM @SelectedObjects SelectedObjects
        WHERE DatabaseName = @CurrentDatabaseName
        AND SchemaName NOT LIKE '%[%]%'
        AND ObjectName NOT LIKE '%[%]%'
        AND NOT EXISTS (SELECT * FROM @tmpObjects WHERE SchemaName = SelectedObjects.SchemaName AND ObjectName = SelectedObjects.ObjectName)
        IF @@ROWCOUNT > 0
        BEGIN
          SET @ErrorMessage = 'The following objects do not exist: ' + LEFT(@ErrorMessage,LEN(@ErrorMessage)-1) + '.'
          RAISERROR('%s',10,1,@ErrorMessage) WITH NOWAIT
          SET @Error = @@ERROR
          RAISERROR(@EmptyLine,10,1) WITH NOWAIT
        END

        WHILE (SYSDATETIME() < DATEADD(SECOND,@TimeLimit,@StartTime) OR @TimeLimit IS NULL)
        BEGIN
          SELECT TOP 1 @CurrentOID = ID,
                       @CurrentSchemaID = SchemaID,
                       @CurrentSchemaName = SchemaName,
                       @CurrentObjectID = ObjectID,
                       @CurrentObjectName = ObjectName,
                       @CurrentObjectType = ObjectType
          FROM @tmpObjects
          WHERE Selected = 1
          AND Completed = 0
          ORDER BY [Order] ASC

          IF @@ROWCOUNT = 0
          BEGIN
            BREAK
          END

          -- Does the object exist?
          SET @CurrentCommand = ''
          IF @LockTimeout IS NOT NULL SET @CurrentCommand = 'SET LOCK_TIMEOUT ' + CAST(@LockTimeout * 1000 AS nvarchar) + '; '
          SET @CurrentCommand += 'IF EXISTS(SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.schema_id = schemas.schema_id LEFT OUTER JOIN sys.tables tables ON objects.object_id = tables.object_id WHERE objects.[type] IN(''U'',''V'') AND EXISTS(SELECT * FROM sys.indexes indexes WHERE indexes.object_id = objects.object_id)' + CASE WHEN @Version >= 12 THEN ' AND (tables.is_memory_optimized = 0 OR is_memory_optimized IS NULL)' ELSE '' END + ' AND schemas.[schema_id] = @ParamSchemaID AND schemas.[name] = @ParamSchemaName AND objects.[object_id] = @ParamObjectID AND objects.[name] = @ParamObjectName AND objects.[type] = @ParamObjectType) BEGIN SET @ParamObjectExists = 1 END'

          BEGIN TRY
            EXECUTE @CurrentDatabase_sp_executesql @stmt = @CurrentCommand, @params = N'@ParamSchemaID int, @ParamSchemaName sysname, @ParamObjectID int, @ParamObjectName sysname, @ParamObjectType sysname, @ParamObjectExists bit OUTPUT', @ParamSchemaID = @CurrentSchemaID, @ParamSchemaName = @CurrentSchemaName, @ParamObjectID = @CurrentObjectID, @ParamObjectName = @CurrentObjectName, @ParamObjectType = @CurrentObjectType, @ParamObjectExists = @CurrentObjectExists OUTPUT

            IF @CurrentObjectExists IS NULL SET @CurrentObjectExists = 0
          END TRY
          BEGIN CATCH
            SET @ErrorMessage = 'Msg ' + CAST(ERROR_NUMBER() AS nvarchar) + ', ' + ISNULL(ERROR_MESSAGE(),'') + CASE WHEN ERROR_NUMBER() = 1222 THEN ', ' + 'The object ' + QUOTENAME(@CurrentDatabaseName) + '.' + QUOTENAME(@CurrentSchemaName) + '.' + QUOTENAME(@CurrentObjectName) + ' is locked. It could not be checked if the object exists.' ELSE '' END
            SET @Severity = CASE WHEN ERROR_NUMBER() IN(1205,1222) THEN @LockMessageSeverity ELSE 16 END
            RAISERROR('%s',@Severity,1,@ErrorMessage) WITH NOWAIT
            RAISERROR(@EmptyLine,10,1) WITH NOWAIT

            IF NOT (ERROR_NUMBER() IN(1205,1222) AND @LockMessageSeverity = 10)
            BEGIN
              SET @ReturnCode = ERROR_NUMBER()
            END
          END CATCH

          IF @CurrentObjectExists = 1
          BEGIN
            SET @CurrentDatabaseContext = @CurrentDatabaseName

            SET @CurrentCommandType = 'DBCC_CHECKTABLE'

            SET @CurrentCommand = ''
            IF @LockTimeout IS NOT NULL SET @CurrentCommand = 'SET LOCK_TIMEOUT ' + CAST(@LockTimeout * 1000 AS nvarchar) + '; '
            SET @CurrentCommand += 'DBCC CHECKTABLE (''' + QUOTENAME(@CurrentSchemaName) + '.' + QUOTENAME(@CurrentObjectName) + ''''
            IF @NoIndex = 'Y' SET @CurrentCommand += ', NOINDEX'
            SET @CurrentCommand += ') WITH NO_INFOMSGS, ALL_ERRORMSGS'
            IF @DataPurity = 'Y' SET @CurrentCommand += ', DATA_PURITY'
            IF @PhysicalOnly = 'Y' SET @CurrentCommand += ', PHYSICAL_ONLY'
            IF @ExtendedLogicalChecks = 'Y' SET @CurrentCommand += ', EXTENDED_LOGICAL_CHECKS'
            IF @TabLock = 'Y' SET @CurrentCommand += ', TABLOCK'
            IF @MaxDOP IS NOT NULL SET @CurrentCommand += ', MAXDOP = ' + CAST(@MaxDOP AS nvarchar)

            EXECUTE @CurrentCommandOutput = dbo.CommandExecute @DatabaseContext = @CurrentDatabaseContext, @Command = @CurrentCommand, @CommandType = @CurrentCommandType, @Mode = 1, @DatabaseName = @CurrentDatabaseName, @SchemaName = @CurrentSchemaName, @ObjectName = @CurrentObjectName, @ObjectType = @CurrentObjectType, @LogToTable = @LogToTable, @Execute = @Execute
            SET @Error = @@ERROR
            IF @Error <> 0 SET @CurrentCommandOutput = @Error
            IF @CurrentCommandOutput <> 0 SET @ReturnCode = @CurrentCommandOutput
          END

          UPDATE @tmpObjects
          SET Completed = 1
          WHERE Selected = 1
          AND Completed = 0
          AND ID = @CurrentOID

          SET @CurrentOID = NULL
          SET @CurrentSchemaID = NULL
          SET @CurrentSchemaName = NULL
          SET @CurrentObjectID = NULL
          SET @CurrentObjectName = NULL
          SET @CurrentObjectType = NULL
          SET @CurrentObjectExists = NULL

          SET @CurrentDatabaseContext = NULL
          SET @CurrentCommand = NULL
          SET @CurrentCommandOutput = NULL
          SET @CurrentCommandType = NULL
        END
      END

      -- Check catalog
      IF EXISTS(SELECT * FROM @SelectedCheckCommands WHERE CheckCommand = 'CHECKCATALOG') AND (SYSDATETIME() < DATEADD(SECOND,@TimeLimit,@StartTime) OR @TimeLimit IS NULL)
      BEGIN
        SET @CurrentDatabaseContext = CASE WHEN SERVERPROPERTY('EngineEdition') = 5 THEN @CurrentDatabaseName ELSE 'master' END

        SET @CurrentCommandType = 'DBCC_CHECKCATALOG'

        SET @CurrentCommand = ''
        IF @LockTimeout IS NOT NULL SET @CurrentCommand = 'SET LOCK_TIMEOUT ' + CAST(@LockTimeout * 1000 AS nvarchar) + '; '
        SET @CurrentCommand += 'DBCC CHECKCATALOG (' + QUOTENAME(@CurrentDatabaseName)
        SET @CurrentCommand += ') WITH NO_INFOMSGS'

        EXECUTE @CurrentCommandOutput = dbo.CommandExecute @DatabaseContext = @CurrentDatabaseContext, @Command = @CurrentCommand, @CommandType = @CurrentCommandType, @Mode = 1, @DatabaseName = @CurrentDatabaseName, @LogToTable = @LogToTable, @Execute = @Execute
        SET @Error = @@ERROR
        IF @Error <> 0 SET @CurrentCommandOutput = @Error
        IF @CurrentCommandOutput <> 0 SET @ReturnCode = @CurrentCommandOutput
      END

    END

    IF @CurrentDatabaseState = 'SUSPECT'
    BEGIN
      SET @ErrorMessage = 'The database ' + QUOTENAME(@CurrentDatabaseName) + ' is in a SUSPECT state.'
      RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
      SET @Error = @@ERROR
      RAISERROR(@EmptyLine,10,1) WITH NOWAIT
    END

    -- Update that the database is completed
    IF @DatabasesInParallel = 'Y'
    BEGIN
      UPDATE dbo.QueueDatabase
      SET DatabaseEndTime = SYSDATETIME()
      WHERE QueueID = @QueueID
      AND DatabaseName = @CurrentDatabaseName
    END
    ELSE
    BEGIN
      UPDATE @tmpDatabases
      SET Completed = 1
      WHERE Selected = 1
      AND Completed = 0
      AND ID = @CurrentDBID
    END

    -- Clear variables
    SET @CurrentDBID = NULL
    SET @CurrentDatabaseName = NULL

    SET @CurrentDatabase_sp_executesql = NULL

    SET @CurrentUserAccess = NULL
    SET @CurrentIsReadOnly = NULL
    SET @CurrentDatabaseState = NULL
    SET @CurrentInStandby = NULL
    SET @CurrentRecoveryModel = NULL

    SET @CurrentIsDatabaseAccessible = NULL
    SET @CurrentReplicaID = NULL
    SET @CurrentAvailabilityGroupID = NULL
    SET @CurrentAvailabilityGroup = NULL
    SET @CurrentAvailabilityGroupRole = NULL
    SET @CurrentAvailabilityGroupBackupPreference = NULL
    SET @CurrentSecondaryRoleAllowConnections = NULL
    SET @CurrentIsPreferredBackupReplica = NULL
    SET @CurrentDatabaseMirroringRole = NULL

    SET @CurrentDatabaseContext = NULL
    SET @CurrentCommand = NULL
    SET @CurrentCommandOutput = NULL
    SET @CurrentCommandType = NULL

    DELETE FROM @tmpFileGroups
    DELETE FROM @tmpObjects

  END

  ----------------------------------------------------------------------------------------------------
  --// Log completing information                                                                 //--
  ----------------------------------------------------------------------------------------------------

  Logging:
  SET @EndMessage = 'Date and time: ' + CONVERT(nvarchar,SYSDATETIME(),120)
  RAISERROR('%s',10,1,@EndMessage) WITH NOWAIT

  RAISERROR(@EmptyLine,10,1) WITH NOWAIT

  IF @ReturnCode <> 0
  BEGIN
    RETURN @ReturnCode
  END

  ----------------------------------------------------------------------------------------------------

END

GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[IndexOptimize]') AND type in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [dbo].[IndexOptimize] AS'
END
GO
ALTER PROCEDURE [dbo].[IndexOptimize]

@Databases nvarchar(max) = NULL,
@FragmentationLow nvarchar(max) = NULL,
@FragmentationMedium nvarchar(max) = 'INDEX_REORGANIZE,INDEX_REBUILD_ONLINE,INDEX_REBUILD_OFFLINE',
@FragmentationHigh nvarchar(max) = 'INDEX_REBUILD_ONLINE,INDEX_REBUILD_OFFLINE',
@FragmentationLevel1 int = 5,
@FragmentationLevel2 int = 30,
@MinNumberOfPages int = 1000,
@MaxNumberOfPages int = NULL,
@SortInTempdb nvarchar(max) = 'N',
@MaxDOP int = NULL,
@FillFactor int = NULL,
@PadIndex nvarchar(max) = NULL,
@LOBCompaction nvarchar(max) = 'Y',
@UpdateStatistics nvarchar(max) = NULL,
@OnlyModifiedStatistics nvarchar(max) = 'N',
@StatisticsModificationLevel int = NULL,
@StatisticsSample int = NULL,
@StatisticsResample nvarchar(max) = 'N',
@PartitionLevel nvarchar(max) = 'Y',
@MSShippedObjects nvarchar(max) = 'N',
@Indexes nvarchar(max) = NULL,
@TimeLimit int = NULL,
@Delay int = NULL,
@WaitAtLowPriorityMaxDuration int = NULL,
@WaitAtLowPriorityAbortAfterWait nvarchar(max) = NULL,
@Resumable nvarchar(max) = 'N',
@AvailabilityGroups nvarchar(max) = NULL,
@LockTimeout int = NULL,
@LockMessageSeverity int = 16,
@StringDelimiter nvarchar(max) = ',',
@DatabaseOrder nvarchar(max) = NULL,
@DatabasesInParallel nvarchar(max) = 'N',
@ExecuteAsUser nvarchar(max) = NULL,
@LogToTable nvarchar(max) = 'N',
@Execute nvarchar(max) = 'Y'

AS

BEGIN

  ----------------------------------------------------------------------------------------------------
  --// Source:  Database Maintenance Solution provided by Ascent Technology PTY (Ltd) //--
  --// Version: 2022-12-03 17:23:44                                                             //--
  ----------------------------------------------------------------------------------------------------

  SET NOCOUNT ON

  SET ARITHABORT ON

  SET NUMERIC_ROUNDABORT OFF

  DECLARE @StartMessage nvarchar(max)
  DECLARE @EndMessage nvarchar(max)
  DECLARE @DatabaseMessage nvarchar(max)
  DECLARE @ErrorMessage nvarchar(max)
  DECLARE @Severity int

  DECLARE @StartTime datetime2 = SYSDATETIME()
  DECLARE @SchemaName nvarchar(max) = OBJECT_SCHEMA_NAME(@@PROCID)
  DECLARE @ObjectName nvarchar(max) = OBJECT_NAME(@@PROCID)
  DECLARE @VersionTimestamp nvarchar(max) = SUBSTRING(OBJECT_DEFINITION(@@PROCID),CHARINDEX('--// Version: ',OBJECT_DEFINITION(@@PROCID)) + LEN('--// Version: ') + 1, 19)
  DECLARE @Parameters nvarchar(max)

  DECLARE @HostPlatform nvarchar(max)

  DECLARE @PartitionLevelStatistics bit

  DECLARE @QueueID int
  DECLARE @QueueStartTime datetime2

  DECLARE @CurrentDBID int
  DECLARE @CurrentDatabaseName nvarchar(max)

  DECLARE @CurrentDatabase_sp_executesql nvarchar(max)

  DECLARE @CurrentExecuteAsUserExists bit
  DECLARE @CurrentUserAccess nvarchar(max)
  DECLARE @CurrentIsReadOnly bit
  DECLARE @CurrentDatabaseState nvarchar(max)
  DECLARE @CurrentInStandby bit
  DECLARE @CurrentRecoveryModel nvarchar(max)

  DECLARE @CurrentIsDatabaseAccessible bit
  DECLARE @CurrentReplicaID uniqueidentifier
  DECLARE @CurrentAvailabilityGroupID uniqueidentifier
  DECLARE @CurrentAvailabilityGroup nvarchar(max)
  DECLARE @CurrentAvailabilityGroupRole nvarchar(max)
  DECLARE @CurrentDatabaseMirroringRole nvarchar(max)

  DECLARE @CurrentDatabaseContext nvarchar(max)
  DECLARE @CurrentCommand nvarchar(max)
  DECLARE @CurrentCommandOutput int
  DECLARE @CurrentCommandType nvarchar(max)
  DECLARE @CurrentComment nvarchar(max)
  DECLARE @CurrentExtendedInfo xml

  DECLARE @Errors TABLE (ID int IDENTITY PRIMARY KEY,
                         [Message] nvarchar(max) NOT NULL,
                         Severity int NOT NULL,
                         [State] int)

  DECLARE @CurrentMessage nvarchar(max)
  DECLARE @CurrentSeverity int
  DECLARE @CurrentState int

  DECLARE @CurrentIxID int
  DECLARE @CurrentIxOrder int
  DECLARE @CurrentSchemaID int
  DECLARE @CurrentSchemaName nvarchar(max)
  DECLARE @CurrentObjectID int
  DECLARE @CurrentObjectName nvarchar(max)
  DECLARE @CurrentObjectType nvarchar(max)
  DECLARE @CurrentIsMemoryOptimized bit
  DECLARE @CurrentIndexID int
  DECLARE @CurrentIndexName nvarchar(max)
  DECLARE @CurrentIndexType int
  DECLARE @CurrentStatisticsID int
  DECLARE @CurrentStatisticsName nvarchar(max)
  DECLARE @CurrentPartitionID bigint
  DECLARE @CurrentPartitionNumber int
  DECLARE @CurrentPartitionCount int
  DECLARE @CurrentIsPartition bit
  DECLARE @CurrentIndexExists bit
  DECLARE @CurrentStatisticsExists bit
  DECLARE @CurrentIsImageText bit
  DECLARE @CurrentIsNewLOB bit
  DECLARE @CurrentIsFileStream bit
  DECLARE @CurrentIsColumnStore bit
  DECLARE @CurrentIsComputed bit
  DECLARE @CurrentIsTimestamp bit
  DECLARE @CurrentAllowPageLocks bit
  DECLARE @CurrentNoRecompute bit
  DECLARE @CurrentIsIncremental bit
  DECLARE @CurrentRowCount bigint
  DECLARE @CurrentModificationCounter bigint
  DECLARE @CurrentOnReadOnlyFileGroup bit
  DECLARE @CurrentResumableIndexOperation bit
  DECLARE @CurrentFragmentationLevel float
  DECLARE @CurrentPageCount bigint
  DECLARE @CurrentFragmentationGroup nvarchar(max)
  DECLARE @CurrentAction nvarchar(max)
  DECLARE @CurrentMaxDOP int
  DECLARE @CurrentUpdateStatistics nvarchar(max)
  DECLARE @CurrentStatisticsSample int
  DECLARE @CurrentStatisticsResample nvarchar(max)
  DECLARE @CurrentDelay datetime

  DECLARE @tmpDatabases TABLE (ID int IDENTITY,
                               DatabaseName nvarchar(max),
                               DatabaseType nvarchar(max),
                               AvailabilityGroup bit,
                               StartPosition int,
                               DatabaseSize bigint,
                               [Order] int,
                               Selected bit,
                               Completed bit,
                               PRIMARY KEY(Selected, Completed, [Order], ID))

  DECLARE @tmpAvailabilityGroups TABLE (ID int IDENTITY PRIMARY KEY,
                                        AvailabilityGroupName nvarchar(max),
                                        StartPosition int,
                                        Selected bit)

  DECLARE @tmpDatabasesAvailabilityGroups TABLE (DatabaseName nvarchar(max),
                                                 AvailabilityGroupName nvarchar(max))

  DECLARE @tmpIndexesStatistics TABLE (ID int IDENTITY,
                                       SchemaID int,
                                       SchemaName nvarchar(max),
                                       ObjectID int,
                                       ObjectName nvarchar(max),
                                       ObjectType nvarchar(max),
                                       IsMemoryOptimized bit,
                                       IndexID int,
                                       IndexName nvarchar(max),
                                       IndexType int,
                                       AllowPageLocks bit,
                                       IsImageText bit,
                                       IsNewLOB bit,
                                       IsFileStream bit,
                                       IsColumnStore bit,
                                       IsComputed bit,
                                       IsTimestamp bit,
                                       OnReadOnlyFileGroup bit,
                                       ResumableIndexOperation bit,
                                       StatisticsID int,
                                       StatisticsName nvarchar(max),
                                       [NoRecompute] bit,
                                       IsIncremental bit,
                                       PartitionID bigint,
                                       PartitionNumber int,
                                       PartitionCount int,
                                       StartPosition int,
                                       [Order] int,
                                       Selected bit,
                                       Completed bit,
                                       PRIMARY KEY(Selected, Completed, [Order], ID))

  DECLARE @SelectedDatabases TABLE (DatabaseName nvarchar(max),
                                    DatabaseType nvarchar(max),
                                    AvailabilityGroup nvarchar(max),
                                    StartPosition int,
                                    Selected bit)

  DECLARE @SelectedAvailabilityGroups TABLE (AvailabilityGroupName nvarchar(max),
                                             StartPosition int,
                                             Selected bit)

  DECLARE @SelectedIndexes TABLE (DatabaseName nvarchar(max),
                                  SchemaName nvarchar(max),
                                  ObjectName nvarchar(max),
                                  IndexName nvarchar(max),
                                  StartPosition int,
                                  Selected bit)

  DECLARE @Actions TABLE ([Action] nvarchar(max))

  INSERT INTO @Actions([Action]) VALUES('INDEX_REBUILD_ONLINE')
  INSERT INTO @Actions([Action]) VALUES('INDEX_REBUILD_OFFLINE')
  INSERT INTO @Actions([Action]) VALUES('INDEX_REORGANIZE')

  DECLARE @ActionsPreferred TABLE (FragmentationGroup nvarchar(max),
                                   [Priority] int,
                                   [Action] nvarchar(max))

  DECLARE @CurrentActionsAllowed TABLE ([Action] nvarchar(max))

  DECLARE @CurrentAlterIndexWithClauseArguments TABLE (ID int IDENTITY,
                                                       Argument nvarchar(max),
                                                       Added bit DEFAULT 0)

  DECLARE @CurrentAlterIndexArgumentID int
  DECLARE @CurrentAlterIndexArgument nvarchar(max)
  DECLARE @CurrentAlterIndexWithClause nvarchar(max)

  DECLARE @CurrentUpdateStatisticsWithClauseArguments TABLE (ID int IDENTITY,
                                                             Argument nvarchar(max),
                                                             Added bit DEFAULT 0)

  DECLARE @CurrentUpdateStatisticsArgumentID int
  DECLARE @CurrentUpdateStatisticsArgument nvarchar(max)
  DECLARE @CurrentUpdateStatisticsWithClause nvarchar(max)

  DECLARE @Error int = 0
  DECLARE @ReturnCode int = 0

  DECLARE @EmptyLine nvarchar(max) = CHAR(9)

  DECLARE @Version numeric(18,10) = CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max)),CHARINDEX('.',CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max))) - 1) + '.' + REPLACE(RIGHT(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max)), LEN(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max))) - CHARINDEX('.',CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max)))),'.','') AS numeric(18,10))

  IF @Version >= 14
  BEGIN
    SELECT @HostPlatform = host_platform
    FROM sys.dm_os_host_info
  END
  ELSE
  BEGIN
    SET @HostPlatform = 'Windows'
  END

  DECLARE @AmazonRDS bit = CASE WHEN DB_ID('rdsadmin') IS NOT NULL AND SUSER_SNAME(0x01) = 'rdsa' THEN 1 ELSE 0 END

  ----------------------------------------------------------------------------------------------------
  --// Log initial information                                                                    //--
  ----------------------------------------------------------------------------------------------------

  SET @Parameters = '@Databases = ' + ISNULL('''' + REPLACE(@Databases,'''','''''') + '''','NULL')
  SET @Parameters += ', @FragmentationLow = ' + ISNULL('''' + REPLACE(@FragmentationLow,'''','''''') + '''','NULL')
  SET @Parameters += ', @FragmentationMedium = ' + ISNULL('''' + REPLACE(@FragmentationMedium,'''','''''') + '''','NULL')
  SET @Parameters += ', @FragmentationHigh = ' + ISNULL('''' + REPLACE(@FragmentationHigh,'''','''''') + '''','NULL')
  SET @Parameters += ', @FragmentationLevel1 = ' + ISNULL(CAST(@FragmentationLevel1 AS nvarchar),'NULL')
  SET @Parameters += ', @FragmentationLevel2 = ' + ISNULL(CAST(@FragmentationLevel2 AS nvarchar),'NULL')
  SET @Parameters += ', @MinNumberOfPages = ' + ISNULL(CAST(@MinNumberOfPages AS nvarchar),'NULL')
  SET @Parameters += ', @MaxNumberOfPages = ' + ISNULL(CAST(@MaxNumberOfPages AS nvarchar),'NULL')
  SET @Parameters += ', @SortInTempdb = ' + ISNULL('''' + REPLACE(@SortInTempdb,'''','''''') + '''','NULL')
  SET @Parameters += ', @MaxDOP = ' + ISNULL(CAST(@MaxDOP AS nvarchar),'NULL')
  SET @Parameters += ', @FillFactor = ' + ISNULL(CAST(@FillFactor AS nvarchar),'NULL')
  SET @Parameters += ', @PadIndex = ' + ISNULL('''' + REPLACE(@PadIndex,'''','''''') + '''','NULL')
  SET @Parameters += ', @LOBCompaction = ' + ISNULL('''' + REPLACE(@LOBCompaction,'''','''''') + '''','NULL')
  SET @Parameters += ', @UpdateStatistics = ' + ISNULL('''' + REPLACE(@UpdateStatistics,'''','''''') + '''','NULL')
  SET @Parameters += ', @OnlyModifiedStatistics = ' + ISNULL('''' + REPLACE(@OnlyModifiedStatistics,'''','''''') + '''','NULL')
  SET @Parameters += ', @StatisticsModificationLevel = ' + ISNULL(CAST(@StatisticsModificationLevel AS nvarchar),'NULL')
  SET @Parameters += ', @StatisticsSample = ' + ISNULL(CAST(@StatisticsSample AS nvarchar),'NULL')
  SET @Parameters += ', @StatisticsResample = ' + ISNULL('''' + REPLACE(@StatisticsResample,'''','''''') + '''','NULL')
  SET @Parameters += ', @PartitionLevel = ' + ISNULL('''' + REPLACE(@PartitionLevel,'''','''''') + '''','NULL')
  SET @Parameters += ', @MSShippedObjects = ' + ISNULL('''' + REPLACE(@MSShippedObjects,'''','''''') + '''','NULL')
  SET @Parameters += ', @Indexes = ' + ISNULL('''' + REPLACE(@Indexes,'''','''''') + '''','NULL')
  SET @Parameters += ', @TimeLimit = ' + ISNULL(CAST(@TimeLimit AS nvarchar),'NULL')
  SET @Parameters += ', @Delay = ' + ISNULL(CAST(@Delay AS nvarchar),'NULL')
  SET @Parameters += ', @WaitAtLowPriorityMaxDuration = ' + ISNULL(CAST(@WaitAtLowPriorityMaxDuration AS nvarchar),'NULL')
  SET @Parameters += ', @WaitAtLowPriorityAbortAfterWait = ' + ISNULL('''' + REPLACE(@WaitAtLowPriorityAbortAfterWait,'''','''''') + '''','NULL')
  SET @Parameters += ', @Resumable = ' + ISNULL('''' + REPLACE(@Resumable,'''','''''') + '''','NULL')
  SET @Parameters += ', @AvailabilityGroups = ' + ISNULL('''' + REPLACE(@AvailabilityGroups,'''','''''') + '''','NULL')
  SET @Parameters += ', @LockTimeout = ' + ISNULL(CAST(@LockTimeout AS nvarchar),'NULL')
  SET @Parameters += ', @LockMessageSeverity = ' + ISNULL(CAST(@LockMessageSeverity AS nvarchar),'NULL')
  SET @Parameters += ', @StringDelimiter = ' + ISNULL('''' + REPLACE(@StringDelimiter,'''','''''') + '''','NULL')
  SET @Parameters += ', @DatabaseOrder = ' + ISNULL('''' + REPLACE(@DatabaseOrder,'''','''''') + '''','NULL')
  SET @Parameters += ', @DatabasesInParallel = ' + ISNULL('''' + REPLACE(@DatabasesInParallel,'''','''''') + '''','NULL')
  SET @Parameters += ', @ExecuteAsUser = ' + ISNULL('''' + REPLACE(@ExecuteAsUser,'''','''''') + '''','NULL')
  SET @Parameters += ', @LogToTable = ' + ISNULL('''' + REPLACE(@LogToTable,'''','''''') + '''','NULL')
  SET @Parameters += ', @Execute = ' + ISNULL('''' + REPLACE(@Execute,'''','''''') + '''','NULL')

  SET @StartMessage = 'Date and time: ' + CONVERT(nvarchar,@StartTime,120)
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Server: ' + CAST(SERVERPROPERTY('ServerName') AS nvarchar(max))
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Version: ' + CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max))
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Edition: ' + CAST(SERVERPROPERTY('Edition') AS nvarchar(max))
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Platform: ' + @HostPlatform
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Procedure: ' + QUOTENAME(DB_NAME(DB_ID())) + '.' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@ObjectName)
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Parameters: ' + @Parameters
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Version: ' + @VersionTimestamp
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Source: Ascent Technology. https://www.ascent.tech '
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  RAISERROR(@EmptyLine,10,1) WITH NOWAIT

  ----------------------------------------------------------------------------------------------------
  --// Check core requirements                                                                    //--
  ----------------------------------------------------------------------------------------------------

  IF NOT (SELECT [compatibility_level] FROM sys.databases WHERE database_id = DB_ID()) >= 90
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The database ' + QUOTENAME(DB_NAME(DB_ID())) + ' has to be in compatibility level 90 or higher.', 16, 1
  END

  IF NOT (SELECT uses_ansi_nulls FROM sys.sql_modules WHERE [object_id] = @@PROCID) = 1
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'ANSI_NULLS has to be set to ON for the stored procedure.', 16, 1
  END

  IF NOT (SELECT uses_quoted_identifier FROM sys.sql_modules WHERE [object_id] = @@PROCID) = 1
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'QUOTED_IDENTIFIER has to be set to ON for the stored procedure.', 16, 1
  END

  IF NOT EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'P' AND schemas.[name] = 'dbo' AND objects.[name] = 'CommandExecute')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The stored procedure CommandExecute is missing. Contact your Ascent Technology Consultant. https://www.ascent.tech ', 16, 1
  END

  IF EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'P' AND schemas.[name] = 'dbo' AND objects.[name] = 'CommandExecute' AND OBJECT_DEFINITION(objects.[object_id]) NOT LIKE '%@DatabaseContext%')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The stored procedure CommandExecute needs to be updated. Contact your Ascent Technology Consultant. https://www.ascent.tech ', 16, 1
  END

  IF @LogToTable = 'Y' AND NOT EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'U' AND schemas.[name] = 'dbo' AND objects.[name] = 'CommandLog')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The table CommandLog is missing. Contact your Ascent Technology Consultant. https://www.ascent.tech .', 16, 1
  END

  IF @DatabasesInParallel = 'Y' AND NOT EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'U' AND schemas.[name] = 'dbo' AND objects.[name] = 'Queue')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The table Queue is missing. Contact your Ascent Technology Consultant. https://www.ascent.tech .', 16, 1
  END

  IF @DatabasesInParallel = 'Y' AND NOT EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'U' AND schemas.[name] = 'dbo' AND objects.[name] = 'QueueDatabase')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The table QueueDatabase is missing. Contact your Ascent Technology Consultant. https://www.ascent.tech .', 16, 1
  END

  IF @@TRANCOUNT <> 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The transaction count is not 0.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------
  --// Select databases                                                                           //--
  ----------------------------------------------------------------------------------------------------

  SET @Databases = REPLACE(@Databases, CHAR(10), '')
  SET @Databases = REPLACE(@Databases, CHAR(13), '')

  WHILE CHARINDEX(@StringDelimiter + ' ', @Databases) > 0 SET @Databases = REPLACE(@Databases, @StringDelimiter + ' ', @StringDelimiter)
  WHILE CHARINDEX(' ' + @StringDelimiter, @Databases) > 0 SET @Databases = REPLACE(@Databases, ' ' + @StringDelimiter, @StringDelimiter)

  SET @Databases = LTRIM(RTRIM(@Databases));

  WITH Databases1 (StartPosition, EndPosition, DatabaseItem) AS
  (
  SELECT 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Databases, 1), 0), LEN(@Databases) + 1) AS EndPosition,
         SUBSTRING(@Databases, 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Databases, 1), 0), LEN(@Databases) + 1) - 1) AS DatabaseItem
  WHERE @Databases IS NOT NULL
  UNION ALL
  SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Databases, EndPosition + 1), 0), LEN(@Databases) + 1) AS EndPosition,
         SUBSTRING(@Databases, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Databases, EndPosition + 1), 0), LEN(@Databases) + 1) - EndPosition - 1) AS DatabaseItem
  FROM Databases1
  WHERE EndPosition < LEN(@Databases) + 1
  ),
  Databases2 (DatabaseItem, StartPosition, Selected) AS
  (
  SELECT CASE WHEN DatabaseItem LIKE '-%' THEN RIGHT(DatabaseItem,LEN(DatabaseItem) - 1) ELSE DatabaseItem END AS DatabaseItem,
         StartPosition,
         CASE WHEN DatabaseItem LIKE '-%' THEN 0 ELSE 1 END AS Selected
  FROM Databases1
  ),
  Databases3 (DatabaseItem, DatabaseType, AvailabilityGroup, StartPosition, Selected) AS
  (
  SELECT CASE WHEN DatabaseItem IN('ALL_DATABASES','SYSTEM_DATABASES','USER_DATABASES','AVAILABILITY_GROUP_DATABASES') THEN '%' ELSE DatabaseItem END AS DatabaseItem,
         CASE WHEN DatabaseItem = 'SYSTEM_DATABASES' THEN 'S' WHEN DatabaseItem = 'USER_DATABASES' THEN 'U' ELSE NULL END AS DatabaseType,
         CASE WHEN DatabaseItem = 'AVAILABILITY_GROUP_DATABASES' THEN 1 ELSE NULL END AvailabilityGroup,
         StartPosition,
         Selected
  FROM Databases2
  ),
  Databases4 (DatabaseName, DatabaseType, AvailabilityGroup, StartPosition, Selected) AS
  (
  SELECT CASE WHEN LEFT(DatabaseItem,1) = '[' AND RIGHT(DatabaseItem,1) = ']' THEN PARSENAME(DatabaseItem,1) ELSE DatabaseItem END AS DatabaseItem,
         DatabaseType,
         AvailabilityGroup,
         StartPosition,
         Selected
  FROM Databases3
  )
  INSERT INTO @SelectedDatabases (DatabaseName, DatabaseType, AvailabilityGroup, StartPosition, Selected)
  SELECT DatabaseName,
         DatabaseType,
         AvailabilityGroup,
         StartPosition,
         Selected
  FROM Databases4
  OPTION (MAXRECURSION 0)

  IF @Version >= 11 AND SERVERPROPERTY('IsHadrEnabled') = 1
  BEGIN
    INSERT INTO @tmpAvailabilityGroups (AvailabilityGroupName, Selected)
    SELECT name AS AvailabilityGroupName,
           0 AS Selected
    FROM sys.availability_groups

    INSERT INTO @tmpDatabasesAvailabilityGroups (DatabaseName, AvailabilityGroupName)
    SELECT databases.name,
           availability_groups.name
    FROM sys.databases databases
    INNER JOIN sys.availability_replicas availability_replicas ON databases.replica_id = availability_replicas.replica_id
    INNER JOIN sys.availability_groups availability_groups ON availability_replicas.group_id = availability_groups.group_id
  END

  INSERT INTO @tmpDatabases (DatabaseName, DatabaseType, AvailabilityGroup, [Order], Selected, Completed)
  SELECT [name] AS DatabaseName,
         CASE WHEN name IN('master','msdb','model') OR is_distributor = 1 THEN 'S' ELSE 'U' END AS DatabaseType,
         NULL AS AvailabilityGroup,
         0 AS [Order],
         0 AS Selected,
         0 AS Completed
  FROM sys.databases
  WHERE [name] <> 'tempdb'
  AND source_database_id IS NULL
  ORDER BY [name] ASC

  UPDATE tmpDatabases
  SET AvailabilityGroup = CASE WHEN EXISTS (SELECT * FROM @tmpDatabasesAvailabilityGroups WHERE DatabaseName = tmpDatabases.DatabaseName) THEN 1 ELSE 0 END
  FROM @tmpDatabases tmpDatabases

  UPDATE tmpDatabases
  SET tmpDatabases.Selected = SelectedDatabases.Selected
  FROM @tmpDatabases tmpDatabases
  INNER JOIN @SelectedDatabases SelectedDatabases
  ON tmpDatabases.DatabaseName LIKE REPLACE(SelectedDatabases.DatabaseName,'_','[_]')
  AND (tmpDatabases.DatabaseType = SelectedDatabases.DatabaseType OR SelectedDatabases.DatabaseType IS NULL)
  AND (tmpDatabases.AvailabilityGroup = SelectedDatabases.AvailabilityGroup OR SelectedDatabases.AvailabilityGroup IS NULL)
  WHERE SelectedDatabases.Selected = 1

  UPDATE tmpDatabases
  SET tmpDatabases.Selected = SelectedDatabases.Selected
  FROM @tmpDatabases tmpDatabases
  INNER JOIN @SelectedDatabases SelectedDatabases
  ON tmpDatabases.DatabaseName LIKE REPLACE(SelectedDatabases.DatabaseName,'_','[_]')
  AND (tmpDatabases.DatabaseType = SelectedDatabases.DatabaseType OR SelectedDatabases.DatabaseType IS NULL)
  AND (tmpDatabases.AvailabilityGroup = SelectedDatabases.AvailabilityGroup OR SelectedDatabases.AvailabilityGroup IS NULL)
  WHERE SelectedDatabases.Selected = 0

  UPDATE tmpDatabases
  SET tmpDatabases.StartPosition = SelectedDatabases2.StartPosition
  FROM @tmpDatabases tmpDatabases
  INNER JOIN (SELECT tmpDatabases.DatabaseName, MIN(SelectedDatabases.StartPosition) AS StartPosition
              FROM @tmpDatabases tmpDatabases
              INNER JOIN @SelectedDatabases SelectedDatabases
              ON tmpDatabases.DatabaseName LIKE REPLACE(SelectedDatabases.DatabaseName,'_','[_]')
              AND (tmpDatabases.DatabaseType = SelectedDatabases.DatabaseType OR SelectedDatabases.DatabaseType IS NULL)
              AND (tmpDatabases.AvailabilityGroup = SelectedDatabases.AvailabilityGroup OR SelectedDatabases.AvailabilityGroup IS NULL)
              WHERE SelectedDatabases.Selected = 1
              GROUP BY tmpDatabases.DatabaseName) SelectedDatabases2
  ON tmpDatabases.DatabaseName = SelectedDatabases2.DatabaseName

  IF @Databases IS NOT NULL AND (NOT EXISTS(SELECT * FROM @SelectedDatabases) OR EXISTS(SELECT * FROM @SelectedDatabases WHERE DatabaseName IS NULL OR DatabaseName = ''))
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Databases is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------
  --// Select availability groups                                                                 //--
  ----------------------------------------------------------------------------------------------------

  IF @AvailabilityGroups IS NOT NULL AND @Version >= 11 AND SERVERPROPERTY('IsHadrEnabled') = 1
  BEGIN

    SET @AvailabilityGroups = REPLACE(@AvailabilityGroups, CHAR(10), '')
    SET @AvailabilityGroups = REPLACE(@AvailabilityGroups, CHAR(13), '')

    WHILE CHARINDEX(@StringDelimiter + ' ', @AvailabilityGroups) > 0 SET @AvailabilityGroups = REPLACE(@AvailabilityGroups, @StringDelimiter + ' ', @StringDelimiter)
    WHILE CHARINDEX(' ' + @StringDelimiter, @AvailabilityGroups) > 0 SET @AvailabilityGroups = REPLACE(@AvailabilityGroups, ' ' + @StringDelimiter, @StringDelimiter)

    SET @AvailabilityGroups = LTRIM(RTRIM(@AvailabilityGroups));

    WITH AvailabilityGroups1 (StartPosition, EndPosition, AvailabilityGroupItem) AS
    (
    SELECT 1 AS StartPosition,
           ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @AvailabilityGroups, 1), 0), LEN(@AvailabilityGroups) + 1) AS EndPosition,
           SUBSTRING(@AvailabilityGroups, 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @AvailabilityGroups, 1), 0), LEN(@AvailabilityGroups) + 1) - 1) AS AvailabilityGroupItem
    WHERE @AvailabilityGroups IS NOT NULL
    UNION ALL
    SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
           ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @AvailabilityGroups, EndPosition + 1), 0), LEN(@AvailabilityGroups) + 1) AS EndPosition,
           SUBSTRING(@AvailabilityGroups, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @AvailabilityGroups, EndPosition + 1), 0), LEN(@AvailabilityGroups) + 1) - EndPosition - 1) AS AvailabilityGroupItem
    FROM AvailabilityGroups1
    WHERE EndPosition < LEN(@AvailabilityGroups) + 1
    ),
    AvailabilityGroups2 (AvailabilityGroupItem, StartPosition, Selected) AS
    (
    SELECT CASE WHEN AvailabilityGroupItem LIKE '-%' THEN RIGHT(AvailabilityGroupItem,LEN(AvailabilityGroupItem) - 1) ELSE AvailabilityGroupItem END AS AvailabilityGroupItem,
           StartPosition,
           CASE WHEN AvailabilityGroupItem LIKE '-%' THEN 0 ELSE 1 END AS Selected
    FROM AvailabilityGroups1
    ),
    AvailabilityGroups3 (AvailabilityGroupItem, StartPosition, Selected) AS
    (
    SELECT CASE WHEN AvailabilityGroupItem = 'ALL_AVAILABILITY_GROUPS' THEN '%' ELSE AvailabilityGroupItem END AS AvailabilityGroupItem,
           StartPosition,
           Selected
    FROM AvailabilityGroups2
    ),
    AvailabilityGroups4 (AvailabilityGroupName, StartPosition, Selected) AS
    (
    SELECT CASE WHEN LEFT(AvailabilityGroupItem,1) = '[' AND RIGHT(AvailabilityGroupItem,1) = ']' THEN PARSENAME(AvailabilityGroupItem,1) ELSE AvailabilityGroupItem END AS AvailabilityGroupItem,
           StartPosition,
           Selected
    FROM AvailabilityGroups3
    )
    INSERT INTO @SelectedAvailabilityGroups (AvailabilityGroupName, StartPosition, Selected)
    SELECT AvailabilityGroupName, StartPosition, Selected
    FROM AvailabilityGroups4
    OPTION (MAXRECURSION 0)

    UPDATE tmpAvailabilityGroups
    SET tmpAvailabilityGroups.Selected = SelectedAvailabilityGroups.Selected
    FROM @tmpAvailabilityGroups tmpAvailabilityGroups
    INNER JOIN @SelectedAvailabilityGroups SelectedAvailabilityGroups
    ON tmpAvailabilityGroups.AvailabilityGroupName LIKE REPLACE(SelectedAvailabilityGroups.AvailabilityGroupName,'_','[_]')
    WHERE SelectedAvailabilityGroups.Selected = 1

    UPDATE tmpAvailabilityGroups
    SET tmpAvailabilityGroups.Selected = SelectedAvailabilityGroups.Selected
    FROM @tmpAvailabilityGroups tmpAvailabilityGroups
    INNER JOIN @SelectedAvailabilityGroups SelectedAvailabilityGroups
    ON tmpAvailabilityGroups.AvailabilityGroupName LIKE REPLACE(SelectedAvailabilityGroups.AvailabilityGroupName,'_','[_]')
    WHERE SelectedAvailabilityGroups.Selected = 0

    UPDATE tmpAvailabilityGroups
    SET tmpAvailabilityGroups.StartPosition = SelectedAvailabilityGroups2.StartPosition
    FROM @tmpAvailabilityGroups tmpAvailabilityGroups
    INNER JOIN (SELECT tmpAvailabilityGroups.AvailabilityGroupName, MIN(SelectedAvailabilityGroups.StartPosition) AS StartPosition
                FROM @tmpAvailabilityGroups tmpAvailabilityGroups
                INNER JOIN @SelectedAvailabilityGroups SelectedAvailabilityGroups
                ON tmpAvailabilityGroups.AvailabilityGroupName LIKE REPLACE(SelectedAvailabilityGroups.AvailabilityGroupName,'_','[_]')
                WHERE SelectedAvailabilityGroups.Selected = 1
                GROUP BY tmpAvailabilityGroups.AvailabilityGroupName) SelectedAvailabilityGroups2
    ON tmpAvailabilityGroups.AvailabilityGroupName = SelectedAvailabilityGroups2.AvailabilityGroupName

    UPDATE tmpDatabases
    SET tmpDatabases.StartPosition = tmpAvailabilityGroups.StartPosition,
        tmpDatabases.Selected = 1
    FROM @tmpDatabases tmpDatabases
    INNER JOIN @tmpDatabasesAvailabilityGroups tmpDatabasesAvailabilityGroups ON tmpDatabases.DatabaseName = tmpDatabasesAvailabilityGroups.DatabaseName
    INNER JOIN @tmpAvailabilityGroups tmpAvailabilityGroups ON tmpDatabasesAvailabilityGroups.AvailabilityGroupName = tmpAvailabilityGroups.AvailabilityGroupName
    WHERE tmpAvailabilityGroups.Selected = 1

  END

  IF @AvailabilityGroups IS NOT NULL AND (NOT EXISTS(SELECT * FROM @SelectedAvailabilityGroups) OR EXISTS(SELECT * FROM @SelectedAvailabilityGroups WHERE AvailabilityGroupName IS NULL OR AvailabilityGroupName = '') OR @Version < 11 OR SERVERPROPERTY('IsHadrEnabled') = 0)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @AvailabilityGroups is not supported.', 16, 1
  END

  IF (@Databases IS NULL AND @AvailabilityGroups IS NULL)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'You need to specify one of the parameters @Databases and @AvailabilityGroups.', 16, 2
  END

  IF (@Databases IS NOT NULL AND @AvailabilityGroups IS NOT NULL)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'You can only specify one of the parameters @Databases and @AvailabilityGroups.', 16, 3
  END

  ----------------------------------------------------------------------------------------------------
  --// Select indexes                                                                             //--
  ----------------------------------------------------------------------------------------------------

  SET @Indexes = REPLACE(@Indexes, CHAR(10), '')
  SET @Indexes = REPLACE(@Indexes, CHAR(13), '')

  WHILE CHARINDEX(@StringDelimiter + ' ', @Indexes) > 0 SET @Indexes = REPLACE(@Indexes, @StringDelimiter + ' ', @StringDelimiter)
  WHILE CHARINDEX(' ' + @StringDelimiter, @Indexes) > 0 SET @Indexes = REPLACE(@Indexes, ' ' + @StringDelimiter, @StringDelimiter)

  SET @Indexes = LTRIM(RTRIM(@Indexes));

  WITH Indexes1 (StartPosition, EndPosition, IndexItem) AS
  (
  SELECT 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Indexes, 1), 0), LEN(@Indexes) + 1) AS EndPosition,
         SUBSTRING(@Indexes, 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Indexes, 1), 0), LEN(@Indexes) + 1) - 1) AS IndexItem
  WHERE @Indexes IS NOT NULL
  UNION ALL
  SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Indexes, EndPosition + 1), 0), LEN(@Indexes) + 1) AS EndPosition,
         SUBSTRING(@Indexes, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Indexes, EndPosition + 1), 0), LEN(@Indexes) + 1) - EndPosition - 1) AS IndexItem
  FROM Indexes1
  WHERE EndPosition < LEN(@Indexes) + 1
  ),
  Indexes2 (IndexItem, StartPosition, Selected) AS
  (
  SELECT CASE WHEN IndexItem LIKE '-%' THEN RIGHT(IndexItem,LEN(IndexItem) - 1) ELSE IndexItem END AS IndexItem,
         StartPosition,
         CASE WHEN IndexItem LIKE '-%' THEN 0 ELSE 1 END AS Selected
  FROM Indexes1
  ),
  Indexes3 (IndexItem, StartPosition, Selected) AS
  (
  SELECT CASE WHEN IndexItem = 'ALL_INDEXES' THEN '%.%.%.%' ELSE IndexItem END AS IndexItem,
         StartPosition,
         Selected
  FROM Indexes2
  ),
  Indexes4 (DatabaseName, SchemaName, ObjectName, IndexName, StartPosition, Selected) AS
  (
  SELECT CASE WHEN PARSENAME(IndexItem,4) IS NULL THEN PARSENAME(IndexItem,3) ELSE PARSENAME(IndexItem,4) END AS DatabaseName,
         CASE WHEN PARSENAME(IndexItem,4) IS NULL THEN PARSENAME(IndexItem,2) ELSE PARSENAME(IndexItem,3) END AS SchemaName,
         CASE WHEN PARSENAME(IndexItem,4) IS NULL THEN PARSENAME(IndexItem,1) ELSE PARSENAME(IndexItem,2) END AS ObjectName,
         CASE WHEN PARSENAME(IndexItem,4) IS NULL THEN '%' ELSE PARSENAME(IndexItem,1) END AS IndexName,
         StartPosition,
         Selected
  FROM Indexes3
  )
  INSERT INTO @SelectedIndexes (DatabaseName, SchemaName, ObjectName, IndexName, StartPosition, Selected)
  SELECT DatabaseName, SchemaName, ObjectName, IndexName, StartPosition, Selected
  FROM Indexes4
  OPTION (MAXRECURSION 0)

  ----------------------------------------------------------------------------------------------------
  --// Select actions                                                                             //--
  ----------------------------------------------------------------------------------------------------

  SET @FragmentationLow = REPLACE(@FragmentationLow, @StringDelimiter + ' ', @StringDelimiter);

  WITH FragmentationLow (StartPosition, EndPosition, [Action]) AS
  (
  SELECT 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @FragmentationLow, 1), 0), LEN(@FragmentationLow) + 1) AS EndPosition,
         SUBSTRING(@FragmentationLow, 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @FragmentationLow, 1), 0), LEN(@FragmentationLow) + 1) - 1) AS [Action]
  WHERE @FragmentationLow IS NOT NULL
  UNION ALL
  SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @FragmentationLow, EndPosition + 1), 0), LEN(@FragmentationLow) + 1) AS EndPosition,
         SUBSTRING(@FragmentationLow, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @FragmentationLow, EndPosition + 1), 0), LEN(@FragmentationLow) + 1) - EndPosition - 1) AS [Action]
  FROM FragmentationLow
  WHERE EndPosition < LEN(@FragmentationLow) + 1
  )
  INSERT INTO @ActionsPreferred(FragmentationGroup, [Priority], [Action])
  SELECT 'Low' AS FragmentationGroup,
         ROW_NUMBER() OVER(ORDER BY StartPosition ASC) AS [Priority],
         [Action]
  FROM FragmentationLow
  OPTION (MAXRECURSION 0)

  SET @FragmentationMedium = REPLACE(@FragmentationMedium, @StringDelimiter + ' ', @StringDelimiter);

  WITH FragmentationMedium (StartPosition, EndPosition, [Action]) AS
  (
  SELECT 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @FragmentationMedium, 1), 0), LEN(@FragmentationMedium) + 1) AS EndPosition,
         SUBSTRING(@FragmentationMedium, 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @FragmentationMedium, 1), 0), LEN(@FragmentationMedium) + 1) - 1) AS [Action]
  WHERE @FragmentationMedium IS NOT NULL
  UNION ALL
  SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @FragmentationMedium, EndPosition + 1), 0), LEN(@FragmentationMedium) + 1) AS EndPosition,
         SUBSTRING(@FragmentationMedium, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @FragmentationMedium, EndPosition + 1), 0), LEN(@FragmentationMedium) + 1) - EndPosition - 1) AS [Action]
  FROM FragmentationMedium
  WHERE EndPosition < LEN(@FragmentationMedium) + 1
  )
  INSERT INTO @ActionsPreferred(FragmentationGroup, [Priority], [Action])
  SELECT 'Medium' AS FragmentationGroup,
         ROW_NUMBER() OVER(ORDER BY StartPosition ASC) AS [Priority],
         [Action]
  FROM FragmentationMedium
  OPTION (MAXRECURSION 0)

  SET @FragmentationHigh = REPLACE(@FragmentationHigh, @StringDelimiter + ' ', @StringDelimiter);

  WITH FragmentationHigh (StartPosition, EndPosition, [Action]) AS
  (
  SELECT 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @FragmentationHigh, 1), 0), LEN(@FragmentationHigh) + 1) AS EndPosition,
         SUBSTRING(@FragmentationHigh, 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @FragmentationHigh, 1), 0), LEN(@FragmentationHigh) + 1) - 1) AS [Action]
  WHERE @FragmentationHigh IS NOT NULL
  UNION ALL
  SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @FragmentationHigh, EndPosition + 1), 0), LEN(@FragmentationHigh) + 1) AS EndPosition,
         SUBSTRING(@FragmentationHigh, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @FragmentationHigh, EndPosition + 1), 0), LEN(@FragmentationHigh) + 1) - EndPosition - 1) AS [Action]
  FROM FragmentationHigh
  WHERE EndPosition < LEN(@FragmentationHigh) + 1
  )
  INSERT INTO @ActionsPreferred(FragmentationGroup, [Priority], [Action])
  SELECT 'High' AS FragmentationGroup,
         ROW_NUMBER() OVER(ORDER BY StartPosition ASC) AS [Priority],
         [Action]
  FROM FragmentationHigh
  OPTION (MAXRECURSION 0)

  ----------------------------------------------------------------------------------------------------
  --// Check input parameters                                                                     //--
  ----------------------------------------------------------------------------------------------------

  IF EXISTS (SELECT [Action] FROM @ActionsPreferred WHERE FragmentationGroup = 'Low' AND [Action] NOT IN(SELECT * FROM @Actions))
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @FragmentationLow is not supported.', 16, 1
  END

  IF EXISTS (SELECT * FROM @ActionsPreferred WHERE FragmentationGroup = 'Low' GROUP BY [Action] HAVING COUNT(*) > 1)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @FragmentationLow is not supported.', 16, 2
  END

  ----------------------------------------------------------------------------------------------------

  IF EXISTS (SELECT [Action] FROM @ActionsPreferred WHERE FragmentationGroup = 'Medium' AND [Action] NOT IN(SELECT * FROM @Actions))
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @FragmentationMedium is not supported.', 16, 1
  END

  IF EXISTS (SELECT * FROM @ActionsPreferred WHERE FragmentationGroup = 'Medium' GROUP BY [Action] HAVING COUNT(*) > 1)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @FragmentationMedium is not supported.', 16, 2
  END

  ----------------------------------------------------------------------------------------------------

  IF EXISTS (SELECT [Action] FROM @ActionsPreferred WHERE FragmentationGroup = 'High' AND [Action] NOT IN(SELECT * FROM @Actions))
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @FragmentationHigh is not supported.', 16, 1
  END

  IF EXISTS (SELECT * FROM @ActionsPreferred WHERE FragmentationGroup = 'High' GROUP BY [Action] HAVING COUNT(*) > 1)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @FragmentationHigh is not supported.', 16, 2
  END

  ----------------------------------------------------------------------------------------------------

  IF @FragmentationLevel1 <= 0 OR @FragmentationLevel1 >= 100 OR @FragmentationLevel1 IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @FragmentationLevel1 is not supported.', 16, 1
  END

  IF @FragmentationLevel1 >= @FragmentationLevel2
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @FragmentationLevel1 is not supported.', 16, 2
  END

  ----------------------------------------------------------------------------------------------------

  IF @FragmentationLevel2 <= 0 OR @FragmentationLevel2 >= 100 OR @FragmentationLevel2 IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @FragmentationLevel2 is not supported.', 16, 1
  END

  IF @FragmentationLevel2 <= @FragmentationLevel1
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @FragmentationLevel2 is not supported.', 16, 2
  END

  ----------------------------------------------------------------------------------------------------

  IF @MinNumberOfPages < 0 OR @MinNumberOfPages IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @MinNumberOfPages is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @MaxNumberOfPages < 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @MaxNumberOfPages is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @SortInTempdb NOT IN('Y','N') OR @SortInTempdb IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @SortInTempdb is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @MaxDOP < 0 OR @MaxDOP > 64
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @MaxDOP is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @FillFactor <= 0 OR @FillFactor > 100
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @FillFactor is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @PadIndex NOT IN('Y','N')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @PadIndex is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @LOBCompaction NOT IN('Y','N') OR @LOBCompaction IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @LOBCompaction is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @UpdateStatistics NOT IN('ALL','COLUMNS','INDEX')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @UpdateStatistics is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @OnlyModifiedStatistics NOT IN('Y','N') OR @OnlyModifiedStatistics IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @OnlyModifiedStatistics is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @StatisticsModificationLevel <= 0 OR @StatisticsModificationLevel > 100
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @StatisticsModificationLevel is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @OnlyModifiedStatistics = 'Y' AND @StatisticsModificationLevel IS NOT NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'You can only specify one of the parameters @OnlyModifiedStatistics and @StatisticsModificationLevel.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @StatisticsSample <= 0 OR @StatisticsSample  > 100
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @StatisticsSample is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @StatisticsResample NOT IN('Y','N') OR @StatisticsResample IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @StatisticsResample is not supported.', 16, 1
  END

  IF @StatisticsResample = 'Y' AND @StatisticsSample IS NOT NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @StatisticsResample is not supported.', 16, 2
  END

  ----------------------------------------------------------------------------------------------------

  IF @PartitionLevel NOT IN('Y','N') OR @PartitionLevel IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @PartitionLevel is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @MSShippedObjects NOT IN('Y','N') OR @MSShippedObjects IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @MSShippedObjects is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF EXISTS(SELECT * FROM @SelectedIndexes WHERE DatabaseName IS NULL OR SchemaName IS NULL OR ObjectName IS NULL OR IndexName IS NULL)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Indexes is not supported.', 16, 1
  END

  IF @Indexes IS NOT NULL AND NOT EXISTS(SELECT * FROM @SelectedIndexes)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Indexes is not supported.', 16, 2
  END

  ----------------------------------------------------------------------------------------------------

  IF @TimeLimit < 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @TimeLimit is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @Delay < 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Delay is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @WaitAtLowPriorityMaxDuration < 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @WaitAtLowPriorityMaxDuration is not supported.', 16, 1
  END

  IF @WaitAtLowPriorityMaxDuration IS NOT NULL AND @Version < 12
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @WaitAtLowPriorityMaxDuration is not supported.', 16, 2
  END

  ----------------------------------------------------------------------------------------------------

  IF @WaitAtLowPriorityAbortAfterWait NOT IN('NONE','SELF','BLOCKERS')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @WaitAtLowPriorityAbortAfterWait is not supported.', 16, 1
  END

  IF @WaitAtLowPriorityAbortAfterWait IS NOT NULL AND @Version < 12
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @WaitAtLowPriorityAbortAfterWait is not supported.', 16, 2
  END

  ----------------------------------------------------------------------------------------------------

  IF (@WaitAtLowPriorityAbortAfterWait IS NOT NULL AND @WaitAtLowPriorityMaxDuration IS NULL) OR (@WaitAtLowPriorityAbortAfterWait IS NULL AND @WaitAtLowPriorityMaxDuration IS NOT NULL)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The parameters @WaitAtLowPriorityMaxDuration and @WaitAtLowPriorityAbortAfterWait can only be used together.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @Resumable NOT IN('Y','N') OR @Resumable IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Resumable is not supported.', 16, 1
  END

  IF @Resumable = 'Y' AND NOT (@Version >= 14 OR SERVERPROPERTY('EngineEdition') IN (5, 8))
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Resumable is not supported.', 16, 2
  END

  IF @Resumable = 'Y' AND @SortInTempdb = 'Y'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'You can only specify one of the parameters @Resumable and @SortInTempdb.', 16, 3
  END

  ----------------------------------------------------------------------------------------------------

  IF @LockTimeout < 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @LockTimeout is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @LockMessageSeverity NOT IN(10, 16)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @LockMessageSeverity is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @StringDelimiter IS NULL OR LEN(@StringDelimiter) > 1
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @StringDelimiter is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @DatabaseOrder NOT IN('DATABASE_NAME_ASC','DATABASE_NAME_DESC','DATABASE_SIZE_ASC','DATABASE_SIZE_DESC')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @DatabaseOrder is not supported.', 16, 1
  END

  IF @DatabaseOrder IS NOT NULL AND SERVERPROPERTY('EngineEdition') = 5
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @DatabaseOrder is not supported.', 16, 2
  END

  ----------------------------------------------------------------------------------------------------

  IF @DatabasesInParallel NOT IN('Y','N') OR @DatabasesInParallel IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @DatabasesInParallel is not supported.', 16, 1
  END

  IF @DatabasesInParallel = 'Y' AND SERVERPROPERTY('EngineEdition') = 5
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @DatabasesInParallel is not supported.', 16, 2
  END

  ----------------------------------------------------------------------------------------------------

  IF LEN(@ExecuteAsUser) > 128
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @ExecuteAsUser is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @LogToTable NOT IN('Y','N') OR @LogToTable IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @LogToTable is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF @Execute NOT IN('Y','N') OR @Execute IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The value for the parameter @Execute is not supported.', 16, 1
  END

  ----------------------------------------------------------------------------------------------------

  IF EXISTS(SELECT * FROM @Errors)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The documentation is available from Ascent Technology. https://www.ascent.tech ', 16, 1
  END

  ----------------------------------------------------------------------------------------------------
  --// Check that selected databases and availability groups exist                                //--
  ----------------------------------------------------------------------------------------------------

  SET @ErrorMessage = ''
  SELECT @ErrorMessage = @ErrorMessage + QUOTENAME(DatabaseName) + ', '
  FROM @SelectedDatabases
  WHERE DatabaseName NOT LIKE '%[%]%'
  AND DatabaseName NOT IN (SELECT DatabaseName FROM @tmpDatabases)
  IF @@ROWCOUNT > 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The following databases in the @Databases parameter do not exist: ' + LEFT(@ErrorMessage,LEN(@ErrorMessage)-1) + '.', 10, 1
  END

  SET @ErrorMessage = ''
  SELECT @ErrorMessage = @ErrorMessage + QUOTENAME(DatabaseName) + ', '
  FROM @SelectedIndexes
  WHERE DatabaseName NOT LIKE '%[%]%'
  AND DatabaseName NOT IN (SELECT DatabaseName FROM @tmpDatabases)
  IF @@ROWCOUNT > 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The following databases in the @Indexes parameter do not exist: ' + LEFT(@ErrorMessage,LEN(@ErrorMessage)-1) + '.', 10, 1
  END

  SET @ErrorMessage = ''
  SELECT @ErrorMessage = @ErrorMessage + QUOTENAME(AvailabilityGroupName) + ', '
  FROM @SelectedAvailabilityGroups
  WHERE AvailabilityGroupName NOT LIKE '%[%]%'
  AND AvailabilityGroupName NOT IN (SELECT AvailabilityGroupName FROM @tmpAvailabilityGroups)
  IF @@ROWCOUNT > 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The following availability groups do not exist: ' + LEFT(@ErrorMessage,LEN(@ErrorMessage)-1) + '.', 10, 1
  END

  SET @ErrorMessage = ''
  SELECT @ErrorMessage = @ErrorMessage + QUOTENAME(DatabaseName) + ', '
  FROM @SelectedIndexes
  WHERE DatabaseName NOT LIKE '%[%]%'
  AND DatabaseName IN (SELECT DatabaseName FROM @tmpDatabases)
  AND DatabaseName NOT IN (SELECT DatabaseName FROM @tmpDatabases WHERE Selected = 1)
  IF @@ROWCOUNT > 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    SELECT 'The following databases have been selected in the @Indexes parameter, but not in the @Databases or @AvailabilityGroups parameters: ' + LEFT(@ErrorMessage,LEN(@ErrorMessage)-1) + '.', 10, 1
  END

  ----------------------------------------------------------------------------------------------------
  --// Raise errors                                                                               //--
  ----------------------------------------------------------------------------------------------------

  DECLARE ErrorCursor CURSOR FAST_FORWARD FOR SELECT [Message], Severity, [State] FROM @Errors ORDER BY [ID] ASC

  OPEN ErrorCursor

  FETCH ErrorCursor INTO @CurrentMessage, @CurrentSeverity, @CurrentState

  WHILE @@FETCH_STATUS = 0
  BEGIN
    RAISERROR('%s', @CurrentSeverity, @CurrentState, @CurrentMessage) WITH NOWAIT
    RAISERROR(@EmptyLine, 10, 1) WITH NOWAIT

    FETCH NEXT FROM ErrorCursor INTO @CurrentMessage, @CurrentSeverity, @CurrentState
  END

  CLOSE ErrorCursor

  DEALLOCATE ErrorCursor

  IF EXISTS (SELECT * FROM @Errors WHERE Severity >= 16)
  BEGIN
    SET @ReturnCode = 50000
    GOTO Logging
  END

  ----------------------------------------------------------------------------------------------------
  --// Should statistics be updated on the partition level?                                       //--
  ----------------------------------------------------------------------------------------------------

  SET @PartitionLevelStatistics = CASE WHEN @PartitionLevel = 'Y' AND ((@Version >= 12.05 AND @Version < 13) OR @Version >= 13.04422 OR SERVERPROPERTY('EngineEdition') IN (5,8)) THEN 1 ELSE 0 END

  ----------------------------------------------------------------------------------------------------
  --// Update database order                                                                      //--
  ----------------------------------------------------------------------------------------------------

  IF @DatabaseOrder IN('DATABASE_SIZE_ASC','DATABASE_SIZE_DESC')
  BEGIN
    UPDATE tmpDatabases
    SET DatabaseSize = (SELECT SUM(CAST(size AS bigint)) FROM sys.master_files WHERE [type] = 0 AND database_id = DB_ID(tmpDatabases.DatabaseName))
    FROM @tmpDatabases tmpDatabases
  END

  IF @DatabaseOrder IS NULL
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY StartPosition ASC, DatabaseName ASC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'DATABASE_NAME_ASC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY DatabaseName ASC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'DATABASE_NAME_DESC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY DatabaseName DESC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'DATABASE_SIZE_ASC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY DatabaseSize ASC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'DATABASE_SIZE_DESC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY DatabaseSize DESC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END

  ----------------------------------------------------------------------------------------------------
  --// Update the queue                                                                           //--
  ----------------------------------------------------------------------------------------------------

  IF @DatabasesInParallel = 'Y'
  BEGIN

    BEGIN TRY

      SELECT @QueueID = QueueID
      FROM dbo.[Queue]
      WHERE SchemaName = @SchemaName
      AND ObjectName = @ObjectName
      AND [Parameters] = @Parameters

      IF @QueueID IS NULL
      BEGIN
        BEGIN TRANSACTION

        SELECT @QueueID = QueueID
        FROM dbo.[Queue] WITH (UPDLOCK, HOLDLOCK)
        WHERE SchemaName = @SchemaName
        AND ObjectName = @ObjectName
        AND [Parameters] = @Parameters

        IF @QueueID IS NULL
        BEGIN
          INSERT INTO dbo.[Queue] (SchemaName, ObjectName, [Parameters])
          SELECT @SchemaName, @ObjectName, @Parameters

          SET @QueueID = SCOPE_IDENTITY()
        END

        COMMIT TRANSACTION
      END

      BEGIN TRANSACTION

      UPDATE [Queue]
      SET QueueStartTime = SYSDATETIME(),
          SessionID = @@SPID,
          RequestID = (SELECT request_id FROM sys.dm_exec_requests WHERE session_id = @@SPID),
          RequestStartTime = (SELECT start_time FROM sys.dm_exec_requests WHERE session_id = @@SPID)
      FROM dbo.[Queue] [Queue]
      WHERE QueueID = @QueueID
      AND NOT EXISTS (SELECT *
                      FROM sys.dm_exec_requests
                      WHERE session_id = [Queue].SessionID
                      AND request_id = [Queue].RequestID
                      AND start_time = [Queue].RequestStartTime)
      AND NOT EXISTS (SELECT *
                      FROM dbo.QueueDatabase QueueDatabase
                      INNER JOIN sys.dm_exec_requests ON QueueDatabase.SessionID = session_id AND QueueDatabase.RequestID = request_id AND QueueDatabase.RequestStartTime = start_time
                      WHERE QueueDatabase.QueueID = @QueueID)

      IF @@ROWCOUNT = 1
      BEGIN
        INSERT INTO dbo.QueueDatabase (QueueID, DatabaseName)
        SELECT @QueueID AS QueueID,
               DatabaseName
        FROM @tmpDatabases tmpDatabases
        WHERE Selected = 1
        AND NOT EXISTS (SELECT * FROM dbo.QueueDatabase WHERE DatabaseName = tmpDatabases.DatabaseName AND QueueID = @QueueID)

        DELETE QueueDatabase
        FROM dbo.QueueDatabase QueueDatabase
        WHERE QueueID = @QueueID
        AND NOT EXISTS (SELECT * FROM @tmpDatabases tmpDatabases WHERE DatabaseName = QueueDatabase.DatabaseName AND Selected = 1)

        UPDATE QueueDatabase
        SET DatabaseOrder = tmpDatabases.[Order]
        FROM dbo.QueueDatabase QueueDatabase
        INNER JOIN @tmpDatabases tmpDatabases ON QueueDatabase.DatabaseName = tmpDatabases.DatabaseName
        WHERE QueueID = @QueueID
      END

      COMMIT TRANSACTION

      SELECT @QueueStartTime = QueueStartTime
      FROM dbo.[Queue]
      WHERE QueueID = @QueueID

    END TRY

    BEGIN CATCH
      IF XACT_STATE() <> 0
      BEGIN
        ROLLBACK TRANSACTION
      END
      SET @ErrorMessage = 'Msg ' + CAST(ERROR_NUMBER() AS nvarchar) + ', ' + ISNULL(ERROR_MESSAGE(),'')
      RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
      RAISERROR(@EmptyLine,10,1) WITH NOWAIT
      SET @ReturnCode = ERROR_NUMBER()
      GOTO Logging
    END CATCH

  END

  ----------------------------------------------------------------------------------------------------
  --// Execute commands                                                                           //--
  ----------------------------------------------------------------------------------------------------

  WHILE (1 = 1)
  BEGIN

    IF @DatabasesInParallel = 'Y'
    BEGIN
      UPDATE QueueDatabase
      SET DatabaseStartTime = NULL,
          SessionID = NULL,
          RequestID = NULL,
          RequestStartTime = NULL
      FROM dbo.QueueDatabase QueueDatabase
      WHERE QueueID = @QueueID
      AND DatabaseStartTime IS NOT NULL
      AND DatabaseEndTime IS NULL
      AND NOT EXISTS (SELECT * FROM sys.dm_exec_requests WHERE session_id = QueueDatabase.SessionID AND request_id = QueueDatabase.RequestID AND start_time = QueueDatabase.RequestStartTime)

      UPDATE QueueDatabase
      SET DatabaseStartTime = SYSDATETIME(),
          DatabaseEndTime = NULL,
          SessionID = @@SPID,
          RequestID = (SELECT request_id FROM sys.dm_exec_requests WHERE session_id = @@SPID),
          RequestStartTime = (SELECT start_time FROM sys.dm_exec_requests WHERE session_id = @@SPID),
          @CurrentDatabaseName = DatabaseName
      FROM (SELECT TOP 1 DatabaseStartTime,
                         DatabaseEndTime,
                         SessionID,
                         RequestID,
                         RequestStartTime,
                         DatabaseName
            FROM dbo.QueueDatabase
            WHERE QueueID = @QueueID
            AND (DatabaseStartTime < @QueueStartTime OR DatabaseStartTime IS NULL)
            AND NOT (DatabaseStartTime IS NOT NULL AND DatabaseEndTime IS NULL)
            ORDER BY DatabaseOrder ASC
            ) QueueDatabase
    END
    ELSE
    BEGIN
      SELECT TOP 1 @CurrentDBID = ID,
                   @CurrentDatabaseName = DatabaseName
      FROM @tmpDatabases
      WHERE Selected = 1
      AND Completed = 0
      ORDER BY [Order] ASC
    END

    IF @@ROWCOUNT = 0
    BEGIN
      BREAK
    END

    SET @CurrentDatabase_sp_executesql = QUOTENAME(@CurrentDatabaseName) + '.sys.sp_executesql'

    IF @ExecuteAsUser IS NOT NULL
    BEGIN
      SET @CurrentCommand = ''
      SET @CurrentCommand += 'IF EXISTS(SELECT * FROM sys.database_principals database_principals WHERE database_principals.[name] = @ParamExecuteAsUser) BEGIN SET @ParamExecuteAsUserExists = 1 END ELSE BEGIN SET @ParamExecuteAsUserExists = 0 END'

      EXECUTE @CurrentDatabase_sp_executesql @stmt = @CurrentCommand, @params = N'@ParamExecuteAsUser sysname, @ParamExecuteAsUserExists bit OUTPUT', @ParamExecuteAsUser = @ExecuteAsUser, @ParamExecuteAsUserExists = @CurrentExecuteAsUserExists OUTPUT
    END

    BEGIN
      SET @DatabaseMessage = 'Date and time: ' + CONVERT(nvarchar,SYSDATETIME(),120)
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Database: ' + QUOTENAME(@CurrentDatabaseName)
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
    END

    SELECT @CurrentUserAccess = user_access_desc,
           @CurrentIsReadOnly = is_read_only,
           @CurrentDatabaseState = state_desc,
           @CurrentInStandby = is_in_standby,
           @CurrentRecoveryModel = recovery_model_desc
    FROM sys.databases
    WHERE [name] = @CurrentDatabaseName

    BEGIN
      SET @DatabaseMessage = 'State: ' + @CurrentDatabaseState
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Standby: ' + CASE WHEN @CurrentInStandby = 1 THEN 'Yes' ELSE 'No' END
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Updateability: ' + CASE WHEN @CurrentIsReadOnly = 1 THEN 'READ_ONLY' WHEN  @CurrentIsReadOnly = 0 THEN 'READ_WRITE' END
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'User access: ' + @CurrentUserAccess
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Recovery model: ' + @CurrentRecoveryModel
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
    END

    IF @CurrentDatabaseState = 'ONLINE' AND SERVERPROPERTY('EngineEdition') <> 5
    BEGIN
      IF EXISTS (SELECT * FROM sys.database_recovery_status WHERE database_id = DB_ID(@CurrentDatabaseName) AND database_guid IS NOT NULL)
      BEGIN
        SET @CurrentIsDatabaseAccessible = 1
      END
      ELSE
      BEGIN
        SET @CurrentIsDatabaseAccessible = 0
      END
    END

    IF @Version >= 11 AND SERVERPROPERTY('IsHadrEnabled') = 1
    BEGIN
      SELECT @CurrentReplicaID = databases.replica_id
      FROM sys.databases databases
      INNER JOIN sys.availability_replicas availability_replicas ON databases.replica_id = availability_replicas.replica_id
      WHERE databases.[name] = @CurrentDatabaseName

      SELECT @CurrentAvailabilityGroupID = group_id
      FROM sys.availability_replicas
      WHERE replica_id = @CurrentReplicaID

      SELECT @CurrentAvailabilityGroupRole = role_desc
      FROM sys.dm_hadr_availability_replica_states
      WHERE replica_id = @CurrentReplicaID

      SELECT @CurrentAvailabilityGroup = [name]
      FROM sys.availability_groups
      WHERE group_id = @CurrentAvailabilityGroupID
    END

    IF SERVERPROPERTY('EngineEdition') <> 5
    BEGIN
      SELECT @CurrentDatabaseMirroringRole = UPPER(mirroring_role_desc)
      FROM sys.database_mirroring
      WHERE database_id = DB_ID(@CurrentDatabaseName)
    END

    IF @CurrentIsDatabaseAccessible IS NOT NULL
    BEGIN
      SET @DatabaseMessage = 'Is accessible: ' + CASE WHEN @CurrentIsDatabaseAccessible = 1 THEN 'Yes' ELSE 'No' END
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
    END

    IF @CurrentAvailabilityGroup IS NOT NULL
    BEGIN
      SET @DatabaseMessage = 'Availability group: ' + ISNULL(@CurrentAvailabilityGroup,'N/A')
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Availability group role: ' + ISNULL(@CurrentAvailabilityGroupRole,'N/A')
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
    END

    IF @CurrentDatabaseMirroringRole IS NOT NULL
    BEGIN
      SET @DatabaseMessage = 'Database mirroring role: ' + @CurrentDatabaseMirroringRole
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
    END

    RAISERROR(@EmptyLine,10,1) WITH NOWAIT

    IF @CurrentExecuteAsUserExists = 0
    BEGIN
      SET @DatabaseMessage = 'The user ' + QUOTENAME(@ExecuteAsUser) + ' does not exist in the database ' + QUOTENAME(@CurrentDatabaseName) + '.'
      RAISERROR('%s',16,1,@DatabaseMessage) WITH NOWAIT
      RAISERROR(@EmptyLine,10,1) WITH NOWAIT
    END

    IF @CurrentDatabaseState = 'ONLINE'
    AND NOT (@CurrentUserAccess = 'SINGLE_USER' AND @CurrentIsDatabaseAccessible = 0)
    AND DATABASEPROPERTYEX(@CurrentDatabaseName,'Updateability') = 'READ_WRITE'
    AND (@CurrentExecuteAsUserExists = 1 OR @CurrentExecuteAsUserExists IS NULL)
    BEGIN

      -- Select indexes in the current database
      IF (EXISTS(SELECT * FROM @ActionsPreferred) OR @UpdateStatistics IS NOT NULL) AND (SYSDATETIME() < DATEADD(SECOND,@TimeLimit,@StartTime) OR @TimeLimit IS NULL)
      BEGIN
        SET @CurrentCommand = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;'
                              + ' SELECT SchemaID, SchemaName, ObjectID, ObjectName, ObjectType, IsMemoryOptimized, IndexID, IndexName, IndexType, AllowPageLocks, IsImageText, IsNewLOB, IsFileStream, IsColumnStore, IsComputed, IsTimestamp, OnReadOnlyFileGroup, ResumableIndexOperation, StatisticsID, StatisticsName, NoRecompute, IsIncremental, PartitionID, PartitionNumber, PartitionCount, [Order], Selected, Completed'
                              + ' FROM ('

        IF EXISTS(SELECT * FROM @ActionsPreferred) OR @UpdateStatistics IN('ALL','INDEX')
        BEGIN
          SET @CurrentCommand = @CurrentCommand + 'SELECT schemas.[schema_id] AS SchemaID'
                                                    + ', schemas.[name] AS SchemaName'
                                                    + ', objects.[object_id] AS ObjectID'
                                                    + ', objects.[name] AS ObjectName'
                                                    + ', RTRIM(objects.[type]) AS ObjectType'
                                                    + ', ' + CASE WHEN @Version >= 12 THEN 'tables.is_memory_optimized' ELSE '0' END + ' AS IsMemoryOptimized'
                                                    + ', indexes.index_id AS IndexID'
                                                    + ', indexes.[name] AS IndexName'
                                                    + ', indexes.[type] AS IndexType'
                                                    + ', indexes.allow_page_locks AS AllowPageLocks'

                                                    + ', CASE WHEN indexes.[type] = 1 AND EXISTS(SELECT * FROM sys.columns columns INNER JOIN sys.types types ON columns.system_type_id = types.user_type_id WHERE columns.[object_id] = objects.object_id AND types.name IN(''image'',''text'',''ntext'')) THEN 1 ELSE 0 END AS IsImageText'

                                                    + ', CASE WHEN indexes.[type] = 1 AND EXISTS(SELECT * FROM sys.columns columns INNER JOIN sys.types types ON columns.system_type_id = types.user_type_id OR (columns.user_type_id = types.user_type_id AND types.is_assembly_type = 1) WHERE columns.[object_id] = objects.object_id AND (types.name IN(''xml'') OR (types.name IN(''varchar'',''nvarchar'',''varbinary'') AND columns.max_length = -1) OR (types.is_assembly_type = 1 AND columns.max_length = -1))) THEN 1'
                                                    + ' WHEN indexes.[type] = 2 AND EXISTS(SELECT * FROM sys.index_columns index_columns INNER JOIN sys.columns columns ON index_columns.[object_id] = columns.[object_id] AND index_columns.column_id = columns.column_id INNER JOIN sys.types types ON columns.system_type_id = types.user_type_id OR (columns.user_type_id = types.user_type_id AND types.is_assembly_type = 1) WHERE index_columns.[object_id] = objects.object_id AND index_columns.index_id = indexes.index_id AND (types.[name] IN(''xml'') OR (types.[name] IN(''varchar'',''nvarchar'',''varbinary'') AND columns.max_length = -1) OR (types.is_assembly_type = 1 AND columns.max_length = -1))) THEN 1 ELSE 0 END AS IsNewLOB'

                                                    + ', CASE WHEN indexes.[type] = 1 AND EXISTS(SELECT * FROM sys.columns columns WHERE columns.[object_id] = objects.object_id  AND columns.is_filestream = 1) THEN 1 ELSE 0 END AS IsFileStream'

                                                    + ', CASE WHEN EXISTS(SELECT * FROM sys.indexes indexes WHERE indexes.[object_id] = objects.object_id AND [type] IN(5,6)) THEN 1 ELSE 0 END AS IsColumnStore'

                                                    + ', CASE WHEN EXISTS(SELECT * FROM sys.index_columns index_columns INNER JOIN sys.columns columns ON index_columns.object_id = columns.object_id AND index_columns.column_id = columns.column_id WHERE (index_columns.key_ordinal > 0 OR index_columns.partition_ordinal > 0) AND columns.is_computed = 1 AND index_columns.object_id = indexes.object_id AND index_columns.index_id = indexes.index_id) THEN 1 ELSE 0 END AS IsComputed'

                                                    + ', CASE WHEN EXISTS(SELECT * FROM sys.index_columns index_columns INNER JOIN sys.columns columns ON index_columns.[object_id] = columns.[object_id] AND index_columns.column_id = columns.column_id INNER JOIN sys.types types ON columns.system_type_id = types.system_type_id WHERE index_columns.[object_id] = objects.object_id AND index_columns.index_id = indexes.index_id AND types.[name] = ''timestamp'') THEN 1 ELSE 0 END AS IsTimestamp'

                                                    + ', CASE WHEN EXISTS (SELECT * FROM sys.indexes indexes2 INNER JOIN sys.destination_data_spaces destination_data_spaces ON indexes.data_space_id = destination_data_spaces.partition_scheme_id INNER JOIN sys.filegroups filegroups ON destination_data_spaces.data_space_id = filegroups.data_space_id WHERE filegroups.is_read_only = 1 AND indexes2.[object_id] = indexes.[object_id] AND indexes2.[index_id] = indexes.index_id' + CASE WHEN @PartitionLevel = 'Y' THEN ' AND destination_data_spaces.destination_id = partitions.partition_number' ELSE '' END + ') THEN 1'
                                                    + ' WHEN EXISTS (SELECT * FROM sys.indexes indexes2 INNER JOIN sys.filegroups filegroups ON indexes.data_space_id = filegroups.data_space_id WHERE filegroups.is_read_only = 1 AND indexes.[object_id] = indexes2.[object_id] AND indexes.[index_id] = indexes2.index_id) THEN 1'
                                                    + ' WHEN indexes.[type] = 1 AND EXISTS (SELECT * FROM sys.tables tables INNER JOIN sys.filegroups filegroups ON tables.lob_data_space_id = filegroups.data_space_id WHERE filegroups.is_read_only = 1 AND tables.[object_id] = objects.[object_id]) THEN 1 ELSE 0 END AS OnReadOnlyFileGroup'

                                                    + ', ' + CASE WHEN @Version >= 14 THEN 'CASE WHEN EXISTS(SELECT * FROM sys.index_resumable_operations index_resumable_operations WHERE state_desc = ''PAUSED'' AND index_resumable_operations.object_id = indexes.object_id AND index_resumable_operations.index_id = indexes.index_id AND (index_resumable_operations.partition_number = partitions.partition_number OR index_resumable_operations.partition_number IS NULL)) THEN 1 ELSE 0 END' ELSE '0' END + ' AS ResumableIndexOperation'

                                                    + ', stats.stats_id AS StatisticsID'
                                                    + ', stats.name AS StatisticsName'
                                                    + ', stats.no_recompute AS NoRecompute'
                                                    + ', ' + CASE WHEN @Version >= 12 THEN 'stats.is_incremental' ELSE '0' END + ' AS IsIncremental'
                                                    + ', ' + CASE WHEN @PartitionLevel = 'Y' THEN 'partitions.partition_id AS PartitionID' WHEN @PartitionLevel = 'N' THEN 'NULL AS PartitionID' END
                                                    + ', ' + CASE WHEN @PartitionLevel = 'Y' THEN 'partitions.partition_number AS PartitionNumber' WHEN @PartitionLevel = 'N' THEN 'NULL AS PartitionNumber' END
                                                    + ', ' + CASE WHEN @PartitionLevel = 'Y' THEN 'IndexPartitions.partition_count AS PartitionCount' WHEN @PartitionLevel = 'N' THEN 'NULL AS PartitionCount' END
                                                    + ', 0 AS [Order]'
                                                    + ', 0 AS Selected'
                                                    + ', 0 AS Completed'
                                                    + ' FROM sys.indexes indexes'
                                                    + ' INNER JOIN sys.objects objects ON indexes.[object_id] = objects.[object_id]'
                                                    + ' INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id]'
                                                    + ' LEFT OUTER JOIN sys.tables tables ON objects.[object_id] = tables.[object_id]'
                                                    + ' LEFT OUTER JOIN sys.stats stats ON indexes.[object_id] = stats.[object_id] AND indexes.[index_id] = stats.[stats_id]'
          IF @PartitionLevel = 'Y'
          BEGIN
            SET @CurrentCommand = @CurrentCommand + ' LEFT OUTER JOIN sys.partitions partitions ON indexes.[object_id] = partitions.[object_id] AND indexes.index_id = partitions.index_id'
                                                      + ' LEFT OUTER JOIN (SELECT partitions.[object_id], partitions.index_id, COUNT(DISTINCT partitions.partition_number) AS partition_count FROM sys.partitions partitions GROUP BY partitions.[object_id], partitions.index_id) IndexPartitions ON partitions.[object_id] = IndexPartitions.[object_id] AND partitions.[index_id] = IndexPartitions.[index_id]'
          END

          SET @CurrentCommand = @CurrentCommand + ' WHERE objects.[type] IN(''U'',''V'')'
                                                    + CASE WHEN @MSShippedObjects = 'N' THEN ' AND objects.is_ms_shipped = 0' ELSE '' END
                                                    + ' AND indexes.[type] IN(1,2,3,4,5,6,7)'
                                                    + ' AND indexes.is_disabled = 0 AND indexes.is_hypothetical = 0'
        END

        IF (EXISTS(SELECT * FROM @ActionsPreferred) AND @UpdateStatistics = 'COLUMNS') OR @UpdateStatistics = 'ALL'
        BEGIN
          SET @CurrentCommand = @CurrentCommand + ' UNION '
        END

        IF @UpdateStatistics IN('ALL','COLUMNS')
        BEGIN
          SET @CurrentCommand = @CurrentCommand + 'SELECT schemas.[schema_id] AS SchemaID'
                                                    + ', schemas.[name] AS SchemaName'
                                                    + ', objects.[object_id] AS ObjectID'
                                                    + ', objects.[name] AS ObjectName'
                                                    + ', RTRIM(objects.[type]) AS ObjectType'
                                                    + ', ' + CASE WHEN @Version >= 12 THEN 'tables.is_memory_optimized' ELSE '0' END + ' AS IsMemoryOptimized'
                                                    + ', NULL AS IndexID, NULL AS IndexName'
                                                    + ', NULL AS IndexType'
                                                    + ', NULL AS AllowPageLocks'
                                                    + ', NULL AS IsImageText'
                                                    + ', NULL AS IsNewLOB'
                                                    + ', NULL AS IsFileStream'
                                                    + ', NULL AS IsColumnStore'
                                                    + ', NULL AS IsComputed'
                                                    + ', NULL AS IsTimestamp'
                                                    + ', NULL AS OnReadOnlyFileGroup'
                                                    + ', NULL AS ResumableIndexOperation'
                                                    + ', stats.stats_id AS StatisticsID'
                                                    + ', stats.name AS StatisticsName'
                                                    + ', stats.no_recompute AS NoRecompute'
                                                    + ', ' + CASE WHEN @Version >= 12 THEN 'stats.is_incremental' ELSE '0' END + ' AS IsIncremental'
                                                    + ', NULL AS PartitionID'
                                                    + ', ' + CASE WHEN @PartitionLevelStatistics = 1 THEN 'dm_db_incremental_stats_properties.partition_number' ELSE 'NULL' END + ' AS PartitionNumber'
                                                    + ', NULL AS PartitionCount'
                                                    + ', 0 AS [Order]'
                                                    + ', 0 AS Selected'
                                                    + ', 0 AS Completed'
                                                    + ' FROM sys.stats stats'
                                                    + ' INNER JOIN sys.objects objects ON stats.[object_id] = objects.[object_id]'
                                                    + ' INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id]'
                                                    + ' LEFT OUTER JOIN sys.tables tables ON objects.[object_id] = tables.[object_id]'

          IF @PartitionLevelStatistics = 1
          BEGIN
            SET @CurrentCommand = @CurrentCommand + ' OUTER APPLY sys.dm_db_incremental_stats_properties(stats.object_id, stats.stats_id) dm_db_incremental_stats_properties'
          END

          SET @CurrentCommand = @CurrentCommand + ' WHERE objects.[type] IN(''U'',''V'')'
                                                    + CASE WHEN @MSShippedObjects = 'N' THEN ' AND objects.is_ms_shipped = 0' ELSE '' END
                                                    + ' AND NOT EXISTS(SELECT * FROM sys.indexes indexes WHERE indexes.[object_id] = stats.[object_id] AND indexes.index_id = stats.stats_id)'
        END

        SET @CurrentCommand = @CurrentCommand + ') IndexesStatistics'

        INSERT INTO @tmpIndexesStatistics (SchemaID, SchemaName, ObjectID, ObjectName, ObjectType, IsMemoryOptimized, IndexID, IndexName, IndexType, AllowPageLocks, IsImageText, IsNewLOB, IsFileStream, IsColumnStore, IsComputed, IsTimestamp, OnReadOnlyFileGroup, ResumableIndexOperation, StatisticsID, StatisticsName, [NoRecompute], IsIncremental, PartitionID, PartitionNumber, PartitionCount, [Order], Selected, Completed)
        EXECUTE @CurrentDatabase_sp_executesql @stmt = @CurrentCommand
        SET @Error = @@ERROR
        IF @Error <> 0
        BEGIN
          SET @ReturnCode = @Error
        END
      END

      IF @Indexes IS NULL
      BEGIN
        UPDATE tmpIndexesStatistics
        SET tmpIndexesStatistics.Selected = 1
        FROM @tmpIndexesStatistics tmpIndexesStatistics
      END
      ELSE
      BEGIN
        UPDATE tmpIndexesStatistics
        SET tmpIndexesStatistics.Selected = SelectedIndexes.Selected
        FROM @tmpIndexesStatistics tmpIndexesStatistics
        INNER JOIN @SelectedIndexes SelectedIndexes
        ON @CurrentDatabaseName LIKE REPLACE(SelectedIndexes.DatabaseName,'_','[_]') AND tmpIndexesStatistics.SchemaName LIKE REPLACE(SelectedIndexes.SchemaName,'_','[_]') AND tmpIndexesStatistics.ObjectName LIKE REPLACE(SelectedIndexes.ObjectName,'_','[_]') AND COALESCE(tmpIndexesStatistics.IndexName,tmpIndexesStatistics.StatisticsName) LIKE REPLACE(SelectedIndexes.IndexName,'_','[_]')
        WHERE SelectedIndexes.Selected = 1

        UPDATE tmpIndexesStatistics
        SET tmpIndexesStatistics.Selected = SelectedIndexes.Selected
        FROM @tmpIndexesStatistics tmpIndexesStatistics
        INNER JOIN @SelectedIndexes SelectedIndexes
        ON @CurrentDatabaseName LIKE REPLACE(SelectedIndexes.DatabaseName,'_','[_]') AND tmpIndexesStatistics.SchemaName LIKE REPLACE(SelectedIndexes.SchemaName,'_','[_]') AND tmpIndexesStatistics.ObjectName LIKE REPLACE(SelectedIndexes.ObjectName,'_','[_]') AND COALESCE(tmpIndexesStatistics.IndexName,tmpIndexesStatistics.StatisticsName) LIKE REPLACE(SelectedIndexes.IndexName,'_','[_]')
        WHERE SelectedIndexes.Selected = 0

        UPDATE tmpIndexesStatistics
        SET tmpIndexesStatistics.StartPosition = SelectedIndexes2.StartPosition
        FROM @tmpIndexesStatistics tmpIndexesStatistics
        INNER JOIN (SELECT tmpIndexesStatistics.SchemaName, tmpIndexesStatistics.ObjectName, tmpIndexesStatistics.IndexName, tmpIndexesStatistics.StatisticsName, MIN(SelectedIndexes.StartPosition) AS StartPosition
                    FROM @tmpIndexesStatistics tmpIndexesStatistics
                    INNER JOIN @SelectedIndexes SelectedIndexes
                    ON @CurrentDatabaseName LIKE REPLACE(SelectedIndexes.DatabaseName,'_','[_]') AND tmpIndexesStatistics.SchemaName LIKE REPLACE(SelectedIndexes.SchemaName,'_','[_]') AND tmpIndexesStatistics.ObjectName LIKE REPLACE(SelectedIndexes.ObjectName,'_','[_]') AND COALESCE(tmpIndexesStatistics.IndexName,tmpIndexesStatistics.StatisticsName) LIKE REPLACE(SelectedIndexes.IndexName,'_','[_]')
                    WHERE SelectedIndexes.Selected = 1
                    GROUP BY tmpIndexesStatistics.SchemaName, tmpIndexesStatistics.ObjectName, tmpIndexesStatistics.IndexName, tmpIndexesStatistics.StatisticsName) SelectedIndexes2
        ON tmpIndexesStatistics.SchemaName = SelectedIndexes2.SchemaName
        AND tmpIndexesStatistics.ObjectName = SelectedIndexes2.ObjectName
        AND (tmpIndexesStatistics.IndexName = SelectedIndexes2.IndexName OR tmpIndexesStatistics.IndexName IS NULL)
        AND (tmpIndexesStatistics.StatisticsName = SelectedIndexes2.StatisticsName OR tmpIndexesStatistics.StatisticsName IS NULL)
      END;

      WITH tmpIndexesStatistics AS (
      SELECT SchemaName, ObjectName, [Order], ROW_NUMBER() OVER (ORDER BY ISNULL(ResumableIndexOperation,0) DESC, StartPosition ASC, SchemaName ASC, ObjectName ASC, CASE WHEN IndexType IS NULL THEN 1 ELSE 0 END ASC, IndexType ASC, IndexName ASC, StatisticsName ASC, PartitionNumber ASC) AS RowNumber
      FROM @tmpIndexesStatistics tmpIndexesStatistics
      WHERE Selected = 1
      )
      UPDATE tmpIndexesStatistics
      SET [Order] = RowNumber

      SET @ErrorMessage = ''
      SELECT @ErrorMessage = @ErrorMessage + QUOTENAME(DatabaseName) + '.' + QUOTENAME(SchemaName) + '.' + QUOTENAME(ObjectName) + ', '
      FROM @SelectedIndexes SelectedIndexes
      WHERE DatabaseName = @CurrentDatabaseName
      AND SchemaName NOT LIKE '%[%]%'
      AND ObjectName NOT LIKE '%[%]%'
      AND IndexName LIKE '%[%]%'
      AND NOT EXISTS (SELECT * FROM @tmpIndexesStatistics WHERE SchemaName = SelectedIndexes.SchemaName AND ObjectName = SelectedIndexes.ObjectName)
      IF @@ROWCOUNT > 0
      BEGIN
        SET @ErrorMessage = 'The following objects in the @Indexes parameter do not exist: ' + LEFT(@ErrorMessage,LEN(@ErrorMessage)-1) + '.'
        RAISERROR('%s',10,1,@ErrorMessage) WITH NOWAIT
        SET @Error = @@ERROR
        RAISERROR(@EmptyLine,10,1) WITH NOWAIT
      END

      SET @ErrorMessage = ''
      SELECT @ErrorMessage = @ErrorMessage + QUOTENAME(DatabaseName) + QUOTENAME(SchemaName) + '.' + QUOTENAME(ObjectName) + '.' + QUOTENAME(IndexName) + ', '
      FROM @SelectedIndexes SelectedIndexes
      WHERE DatabaseName = @CurrentDatabaseName
      AND SchemaName NOT LIKE '%[%]%'
      AND ObjectName NOT LIKE '%[%]%'
      AND IndexName NOT LIKE '%[%]%'
      AND NOT EXISTS (SELECT * FROM @tmpIndexesStatistics WHERE SchemaName = SelectedIndexes.SchemaName AND ObjectName = SelectedIndexes.ObjectName AND IndexName = SelectedIndexes.IndexName)
      IF @@ROWCOUNT > 0
      BEGIN
        SET @ErrorMessage = 'The following indexes in the @Indexes parameter do not exist: ' + LEFT(@ErrorMessage,LEN(@ErrorMessage)-1) + '.'
        RAISERROR('%s',10,1,@ErrorMessage) WITH NOWAIT
        SET @Error = @@ERROR
        RAISERROR(@EmptyLine,10,1) WITH NOWAIT
      END

      WHILE (SYSDATETIME() < DATEADD(SECOND,@TimeLimit,@StartTime) OR @TimeLimit IS NULL)
      BEGIN
        SELECT TOP 1 @CurrentIxID = ID,
                     @CurrentIxOrder = [Order],
                     @CurrentSchemaID = SchemaID,
                     @CurrentSchemaName = SchemaName,
                     @CurrentObjectID = ObjectID,
                     @CurrentObjectName = ObjectName,
                     @CurrentObjectType = ObjectType,
                     @CurrentIsMemoryOptimized = IsMemoryOptimized,
                     @CurrentIndexID = IndexID,
                     @CurrentIndexName = IndexName,
                     @CurrentIndexType = IndexType,
                     @CurrentAllowPageLocks = AllowPageLocks,
                     @CurrentIsImageText = IsImageText,
                     @CurrentIsNewLOB = IsNewLOB,
                     @CurrentIsFileStream = IsFileStream,
                     @CurrentIsColumnStore = IsColumnStore,
                     @CurrentIsComputed = IsComputed,
                     @CurrentIsTimestamp = IsTimestamp,
                     @CurrentOnReadOnlyFileGroup = OnReadOnlyFileGroup,
                     @CurrentResumableIndexOperation = ResumableIndexOperation,
                     @CurrentStatisticsID = StatisticsID,
                     @CurrentStatisticsName = StatisticsName,
                     @CurrentNoRecompute = [NoRecompute],
                     @CurrentIsIncremental = IsIncremental,
                     @CurrentPartitionID = PartitionID,
                     @CurrentPartitionNumber = PartitionNumber,
                     @CurrentPartitionCount = PartitionCount
        FROM @tmpIndexesStatistics
        WHERE Selected = 1
        AND Completed = 0
        ORDER BY [Order] ASC

        IF @@ROWCOUNT = 0
        BEGIN
          BREAK
        END

        -- Is the index a partition?
        IF @CurrentPartitionNumber IS NULL OR @CurrentPartitionCount = 1 BEGIN SET @CurrentIsPartition = 0 END ELSE BEGIN SET @CurrentIsPartition = 1 END

        -- Does the index exist?
        IF @CurrentIndexID IS NOT NULL AND EXISTS(SELECT * FROM @ActionsPreferred)
        BEGIN
          SET @CurrentCommand = ''

          IF @LockTimeout IS NOT NULL SET @CurrentCommand = 'SET LOCK_TIMEOUT ' + CAST(@LockTimeout * 1000 AS nvarchar) + '; '

          IF @CurrentIsPartition = 0 SET @CurrentCommand += 'IF EXISTS(SELECT * FROM sys.indexes indexes INNER JOIN sys.objects objects ON indexes.[object_id] = objects.[object_id] INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] IN(''U'',''V'') AND indexes.[type] IN(1,2,3,4,5,6,7) AND indexes.is_disabled = 0 AND indexes.is_hypothetical = 0 AND schemas.[schema_id] = @ParamSchemaID AND schemas.[name] = @ParamSchemaName AND objects.[object_id] = @ParamObjectID AND objects.[name] = @ParamObjectName AND objects.[type] = @ParamObjectType AND indexes.index_id = @ParamIndexID AND indexes.[name] = @ParamIndexName AND indexes.[type] = @ParamIndexType) BEGIN SET @ParamIndexExists = 1 END'
          IF @CurrentIsPartition = 1 SET @CurrentCommand += 'IF EXISTS(SELECT * FROM sys.indexes indexes INNER JOIN sys.objects objects ON indexes.[object_id] = objects.[object_id] INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] INNER JOIN sys.partitions partitions ON indexes.[object_id] = partitions.[object_id] AND indexes.index_id = partitions.index_id WHERE objects.[type] IN(''U'',''V'') AND indexes.[type] IN(1,2,3,4,5,6,7) AND indexes.is_disabled = 0 AND indexes.is_hypothetical = 0 AND schemas.[schema_id] = @ParamSchemaID AND schemas.[name] = @ParamSchemaName AND objects.[object_id] = @ParamObjectID AND objects.[name] = @ParamObjectName AND objects.[type] = @ParamObjectType AND indexes.index_id = @ParamIndexID AND indexes.[name] = @ParamIndexName AND indexes.[type] = @ParamIndexType AND partitions.partition_id = @ParamPartitionID AND partitions.partition_number = @ParamPartitionNumber) BEGIN SET @ParamIndexExists = 1 END'

          BEGIN TRY
            EXECUTE @CurrentDatabase_sp_executesql @stmt = @CurrentCommand, @params = N'@ParamSchemaID int, @ParamSchemaName sysname, @ParamObjectID int, @ParamObjectName sysname, @ParamObjectType sysname, @ParamIndexID int, @ParamIndexName sysname, @ParamIndexType int, @ParamPartitionID bigint, @ParamPartitionNumber int, @ParamIndexExists bit OUTPUT', @ParamSchemaID = @CurrentSchemaID, @ParamSchemaName = @CurrentSchemaName, @ParamObjectID = @CurrentObjectID, @ParamObjectName = @CurrentObjectName, @ParamObjectType = @CurrentObjectType, @ParamIndexID = @CurrentIndexID, @ParamIndexName = @CurrentIndexName, @ParamIndexType = @CurrentIndexType, @ParamPartitionID = @CurrentPartitionID, @ParamPartitionNumber = @CurrentPartitionNumber, @ParamIndexExists = @CurrentIndexExists OUTPUT

            IF @CurrentIndexExists IS NULL
            BEGIN
              SET @CurrentIndexExists = 0
              GOTO NoAction
            END
          END TRY
          BEGIN CATCH
            SET @ErrorMessage = 'Msg ' + CAST(ERROR_NUMBER() AS nvarchar) + ', ' + ISNULL(ERROR_MESSAGE(),'') + CASE WHEN ERROR_NUMBER() = 1222 THEN ' The index ' + QUOTENAME(@CurrentIndexName) + ' on the object ' + QUOTENAME(@CurrentDatabaseName) + '.' + QUOTENAME(@CurrentSchemaName) + '.' + QUOTENAME(@CurrentObjectName) + ' is locked. It could not be checked if the index exists.' ELSE '' END
            SET @Severity = CASE WHEN ERROR_NUMBER() IN(1205,1222) THEN @LockMessageSeverity ELSE 16 END
            RAISERROR('%s',@Severity,1,@ErrorMessage) WITH NOWAIT
            RAISERROR(@EmptyLine,10,1) WITH NOWAIT

            IF NOT (ERROR_NUMBER() IN(1205,1222) AND @LockMessageSeverity = 10)
            BEGIN
              SET @ReturnCode = ERROR_NUMBER()
            END

            GOTO NoAction
          END CATCH
        END

        -- Does the statistics exist?
        IF @CurrentStatisticsID IS NOT NULL AND @UpdateStatistics IS NOT NULL
        BEGIN
          SET @CurrentCommand = ''

          IF @LockTimeout IS NOT NULL SET @CurrentCommand = 'SET LOCK_TIMEOUT ' + CAST(@LockTimeout * 1000 AS nvarchar) + '; '

          SET @CurrentCommand += 'IF EXISTS(SELECT * FROM sys.stats stats INNER JOIN sys.objects objects ON stats.[object_id] = objects.[object_id] INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] IN(''U'',''V'')' + CASE WHEN @MSShippedObjects = 'N' THEN ' AND objects.is_ms_shipped = 0' ELSE '' END + ' AND schemas.[schema_id] = @ParamSchemaID AND schemas.[name] = @ParamSchemaName AND objects.[object_id] = @ParamObjectID AND objects.[name] = @ParamObjectName AND objects.[type] = @ParamObjectType AND stats.stats_id = @ParamStatisticsID AND stats.[name] = @ParamStatisticsName) BEGIN SET @ParamStatisticsExists = 1 END'

          BEGIN TRY
            EXECUTE @CurrentDatabase_sp_executesql @stmt = @CurrentCommand, @params = N'@ParamSchemaID int, @ParamSchemaName sysname, @ParamObjectID int, @ParamObjectName sysname, @ParamObjectType sysname, @ParamStatisticsID int, @ParamStatisticsName sysname, @ParamStatisticsExists bit OUTPUT', @ParamSchemaID = @CurrentSchemaID, @ParamSchemaName = @CurrentSchemaName, @ParamObjectID = @CurrentObjectID, @ParamObjectName = @CurrentObjectName, @ParamObjectType = @CurrentObjectType, @ParamStatisticsID = @CurrentStatisticsID, @ParamStatisticsName = @CurrentStatisticsName, @ParamStatisticsExists = @CurrentStatisticsExists OUTPUT

            IF @CurrentStatisticsExists IS NULL
            BEGIN
              SET @CurrentStatisticsExists = 0
              GOTO NoAction
            END
          END TRY
          BEGIN CATCH
            SET @ErrorMessage = 'Msg ' + CAST(ERROR_NUMBER() AS nvarchar) + ', ' + ISNULL(ERROR_MESSAGE(),'') + CASE WHEN ERROR_NUMBER() = 1222 THEN ' The statistics ' + QUOTENAME(@CurrentStatisticsName) + ' on the object ' + QUOTENAME(@CurrentDatabaseName) + '.' + QUOTENAME(@CurrentSchemaName) + '.' + QUOTENAME(@CurrentObjectName) + ' is locked. It could not be checked if the statistics exists.' ELSE '' END
            SET @Severity = CASE WHEN ERROR_NUMBER() IN(1205,1222) THEN @LockMessageSeverity ELSE 16 END
            RAISERROR('%s',@Severity,1,@ErrorMessage) WITH NOWAIT
            RAISERROR(@EmptyLine,10,1) WITH NOWAIT

            IF NOT (ERROR_NUMBER() IN(1205,1222) AND @LockMessageSeverity = 10)
            BEGIN
              SET @ReturnCode = ERROR_NUMBER()
            END

            GOTO NoAction
          END CATCH
        END

        -- Has the data in the statistics been modified since the statistics was last updated?
        IF @CurrentStatisticsID IS NOT NULL AND @UpdateStatistics IS NOT NULL
        BEGIN
          SET @CurrentCommand = ''

          IF @LockTimeout IS NOT NULL SET @CurrentCommand = 'SET LOCK_TIMEOUT ' + CAST(@LockTimeout * 1000 AS nvarchar) + '; '

          IF @PartitionLevelStatistics = 1 AND @CurrentIsIncremental = 1
          BEGIN
            SET @CurrentCommand += 'SELECT @ParamRowCount = [rows], @ParamModificationCounter = modification_counter FROM sys.dm_db_incremental_stats_properties (@ParamObjectID, @ParamStatisticsID) WHERE partition_number = @ParamPartitionNumber'
          END
          ELSE
          IF (@Version >= 10.504000 AND @Version < 11) OR @Version >= 11.03000
          BEGIN
            SET @CurrentCommand += 'SELECT @ParamRowCount = [rows], @ParamModificationCounter = modification_counter FROM sys.dm_db_stats_properties (@ParamObjectID, @ParamStatisticsID)'
          END
          ELSE
          BEGIN
            SET @CurrentCommand += 'SELECT @ParamRowCount = rowcnt, @ParamModificationCounter = rowmodctr FROM sys.sysindexes sysindexes WHERE sysindexes.[id] = @ParamObjectID AND sysindexes.[indid] = @ParamStatisticsID'
          END

          BEGIN TRY
            EXECUTE @CurrentDatabase_sp_executesql @stmt = @CurrentCommand, @params = N'@ParamObjectID int, @ParamStatisticsID int, @ParamPartitionNumber int, @ParamRowCount bigint OUTPUT, @ParamModificationCounter bigint OUTPUT', @ParamObjectID = @CurrentObjectID, @ParamStatisticsID = @CurrentStatisticsID, @ParamPartitionNumber = @CurrentPartitionNumber, @ParamRowCount = @CurrentRowCount OUTPUT, @ParamModificationCounter = @CurrentModificationCounter OUTPUT

            IF @CurrentRowCount IS NULL SET @CurrentRowCount = 0
            IF @CurrentModificationCounter IS NULL SET @CurrentModificationCounter = 0
          END TRY
          BEGIN CATCH
            SET @ErrorMessage = 'Msg ' + CAST(ERROR_NUMBER() AS nvarchar) + ', ' + ISNULL(ERROR_MESSAGE(),'') + CASE WHEN ERROR_NUMBER() = 1222 THEN ' The statistics ' + QUOTENAME(@CurrentStatisticsName) + ' on the object ' + QUOTENAME(@CurrentDatabaseName) + '.' + QUOTENAME(@CurrentSchemaName) + '.' + QUOTENAME(@CurrentObjectName) + ' is locked. The rows and modification_counter could not be checked.' ELSE '' END
            SET @Severity = CASE WHEN ERROR_NUMBER() IN(1205,1222) THEN @LockMessageSeverity ELSE 16 END
            RAISERROR('%s',@Severity,1,@ErrorMessage) WITH NOWAIT
            RAISERROR(@EmptyLine,10,1) WITH NOWAIT

            IF NOT (ERROR_NUMBER() IN(1205,1222) AND @LockMessageSeverity = 10)
            BEGIN
              SET @ReturnCode = ERROR_NUMBER()
            END

            GOTO NoAction
          END CATCH
        END

        -- Is the index fragmented?
        IF @CurrentIndexID IS NOT NULL
        AND @CurrentOnReadOnlyFileGroup = 0
        AND EXISTS(SELECT * FROM @ActionsPreferred)
        AND (EXISTS(SELECT [Priority], [Action], COUNT(*) FROM @ActionsPreferred GROUP BY [Priority], [Action] HAVING COUNT(*) <> 3) OR @MinNumberOfPages > 0 OR @MaxNumberOfPages IS NOT NULL)
        BEGIN
          SET @CurrentCommand = ''

          IF @LockTimeout IS NOT NULL SET @CurrentCommand = 'SET LOCK_TIMEOUT ' + CAST(@LockTimeout * 1000 AS nvarchar) + '; '

          SET @CurrentCommand += 'SELECT @ParamFragmentationLevel = MAX(avg_fragmentation_in_percent), @ParamPageCount = SUM(page_count) FROM sys.dm_db_index_physical_stats(DB_ID(@ParamDatabaseName), @ParamObjectID, @ParamIndexID, @ParamPartitionNumber, ''LIMITED'') WHERE alloc_unit_type_desc = ''IN_ROW_DATA'' AND index_level = 0'

          BEGIN TRY
            EXECUTE sp_executesql @stmt = @CurrentCommand, @params = N'@ParamDatabaseName nvarchar(max), @ParamObjectID int, @ParamIndexID int, @ParamPartitionNumber int, @ParamFragmentationLevel float OUTPUT, @ParamPageCount bigint OUTPUT', @ParamDatabaseName = @CurrentDatabaseName, @ParamObjectID = @CurrentObjectID, @ParamIndexID = @CurrentIndexID, @ParamPartitionNumber = @CurrentPartitionNumber, @ParamFragmentationLevel = @CurrentFragmentationLevel OUTPUT, @ParamPageCount = @CurrentPageCount OUTPUT
          END TRY
          BEGIN CATCH
            SET @ErrorMessage = 'Msg ' + CAST(ERROR_NUMBER() AS nvarchar) + ', ' + ISNULL(ERROR_MESSAGE(),'') + CASE WHEN ERROR_NUMBER() = 1222 THEN ' The index ' + QUOTENAME(@CurrentIndexName) + ' on the object ' + QUOTENAME(@CurrentDatabaseName) + '.' + QUOTENAME(@CurrentSchemaName) + '.' + QUOTENAME(@CurrentObjectName) + ' is locked. The page_count and avg_fragmentation_in_percent could not be checked.' ELSE '' END
            SET @Severity = CASE WHEN ERROR_NUMBER() IN(1205,1222) THEN @LockMessageSeverity ELSE 16 END
            RAISERROR('%s',@Severity,1,@ErrorMessage) WITH NOWAIT
            RAISERROR(@EmptyLine,10,1) WITH NOWAIT

            IF NOT (ERROR_NUMBER() IN(1205,1222) AND @LockMessageSeverity = 10)
            BEGIN
              SET @ReturnCode = ERROR_NUMBER()
            END

            GOTO NoAction
          END CATCH
        END

        -- Select fragmentation group
        IF @CurrentIndexID IS NOT NULL AND @CurrentOnReadOnlyFileGroup = 0 AND EXISTS(SELECT * FROM @ActionsPreferred)
        BEGIN
          SET @CurrentFragmentationGroup = CASE
          WHEN @CurrentFragmentationLevel >= @FragmentationLevel2 THEN 'High'
          WHEN @CurrentFragmentationLevel >= @FragmentationLevel1 AND @CurrentFragmentationLevel < @FragmentationLevel2 THEN 'Medium'
          WHEN @CurrentFragmentationLevel < @FragmentationLevel1 THEN 'Low'
          END
        END

        -- Which actions are allowed?
        IF @CurrentIndexID IS NOT NULL AND EXISTS(SELECT * FROM @ActionsPreferred)
        BEGIN
          IF @CurrentOnReadOnlyFileGroup = 0 AND @CurrentIndexType IN (1,2,3,4,5) AND (@CurrentIsMemoryOptimized = 0 OR @CurrentIsMemoryOptimized IS NULL) AND (@CurrentAllowPageLocks = 1 OR @CurrentIndexType = 5)
          BEGIN
            INSERT INTO @CurrentActionsAllowed ([Action])
            VALUES ('INDEX_REORGANIZE')
          END
          IF @CurrentOnReadOnlyFileGroup = 0 AND @CurrentIndexType IN (1,2,3,4,5) AND (@CurrentIsMemoryOptimized = 0 OR @CurrentIsMemoryOptimized IS NULL)
          BEGIN
            INSERT INTO @CurrentActionsAllowed ([Action])
            VALUES ('INDEX_REBUILD_OFFLINE')
          END
          IF @CurrentOnReadOnlyFileGroup = 0
          AND (@CurrentIsMemoryOptimized = 0 OR @CurrentIsMemoryOptimized IS NULL)
          AND (@CurrentIsPartition = 0 OR @Version >= 12)
          AND ((@CurrentIndexType = 1 AND @CurrentIsImageText = 0 AND @CurrentIsNewLOB = 0)
          OR (@CurrentIndexType = 2 AND @CurrentIsNewLOB = 0)
          OR (@CurrentIndexType = 1 AND @CurrentIsImageText = 0 AND @CurrentIsFileStream = 0 AND @Version >= 11)
          OR (@CurrentIndexType = 2 AND @Version >= 11))
          AND (@CurrentIsColumnStore = 0 OR @Version < 11)
          AND SERVERPROPERTY('EngineEdition') IN (3,5,8)
          BEGIN
            INSERT INTO @CurrentActionsAllowed ([Action])
            VALUES ('INDEX_REBUILD_ONLINE')
          END
        END

        -- Decide action
        IF @CurrentIndexID IS NOT NULL
        AND EXISTS(SELECT * FROM @ActionsPreferred)
        AND (@CurrentPageCount >= @MinNumberOfPages OR @MinNumberOfPages = 0)
        AND (@CurrentPageCount <= @MaxNumberOfPages OR @MaxNumberOfPages IS NULL)
        AND @CurrentResumableIndexOperation = 0
        BEGIN
          IF EXISTS(SELECT [Priority], [Action], COUNT(*) FROM @ActionsPreferred GROUP BY [Priority], [Action] HAVING COUNT(*) <> 3)
          BEGIN
            SELECT @CurrentAction = [Action]
            FROM @ActionsPreferred
            WHERE FragmentationGroup = @CurrentFragmentationGroup
            AND [Priority] = (SELECT MIN([Priority])
                              FROM @ActionsPreferred
                              WHERE FragmentationGroup = @CurrentFragmentationGroup
                              AND [Action] IN (SELECT [Action] FROM @CurrentActionsAllowed))
          END
          ELSE
          BEGIN
            SELECT @CurrentAction = [Action]
            FROM @ActionsPreferred
            WHERE [Priority] = (SELECT MIN([Priority])
                                FROM @ActionsPreferred
                                WHERE [Action] IN (SELECT [Action] FROM @CurrentActionsAllowed))
          END
        END

        IF @CurrentResumableIndexOperation = 1
        BEGIN
          SET @CurrentAction = 'INDEX_REBUILD_ONLINE'
        END

        -- Workaround for limitation in SQL Server, http://support.microsoft.com/kb/2292737
        IF @CurrentIndexID IS NOT NULL
        BEGIN
          SET @CurrentMaxDOP = @MaxDOP

          IF @CurrentAction = 'INDEX_REBUILD_ONLINE' AND @CurrentAllowPageLocks = 0
          BEGIN
            SET @CurrentMaxDOP = 1
          END
        END

        -- Update statistics?
        IF @CurrentStatisticsID IS NOT NULL
        AND ((@UpdateStatistics = 'ALL' AND (@CurrentIndexType IN (1,2,3,4,7) OR @CurrentIndexID IS NULL)) OR (@UpdateStatistics = 'INDEX' AND @CurrentIndexID IS NOT NULL AND @CurrentIndexType IN (1,2,3,4,7)) OR (@UpdateStatistics = 'COLUMNS' AND @CurrentIndexID IS NULL))
        AND ((@OnlyModifiedStatistics = 'N' AND @StatisticsModificationLevel IS NULL) OR (@OnlyModifiedStatistics = 'Y' AND @CurrentModificationCounter > 0) OR ((@CurrentModificationCounter * 1. / NULLIF(@CurrentRowCount,0)) * 100 >= @StatisticsModificationLevel) OR (@StatisticsModificationLevel IS NOT NULL AND @CurrentModificationCounter > 0 AND (@CurrentModificationCounter >= SQRT(@CurrentRowCount * 1000))) OR (@CurrentIsMemoryOptimized = 1 AND NOT (@Version >= 13 OR SERVERPROPERTY('EngineEdition') IN (5,8))))
        AND ((@CurrentIsPartition = 0 AND (@CurrentAction NOT IN('INDEX_REBUILD_ONLINE','INDEX_REBUILD_OFFLINE') OR @CurrentAction IS NULL)) OR (@CurrentIsPartition = 1 AND (@CurrentPartitionNumber = @CurrentPartitionCount OR (@PartitionLevelStatistics = 1 AND @CurrentIsIncremental = 1))))
        BEGIN
          SET @CurrentUpdateStatistics = 'Y'
        END
        ELSE
        BEGIN
          SET @CurrentUpdateStatistics = 'N'
        END

        SET @CurrentStatisticsSample = @StatisticsSample
        SET @CurrentStatisticsResample = @StatisticsResample

        -- Memory-optimized tables only supports FULLSCAN and RESAMPLE in SQL Server 2014
        IF @CurrentIsMemoryOptimized = 1 AND NOT (@Version >= 13 OR SERVERPROPERTY('EngineEdition') IN (5,8)) AND (@CurrentStatisticsSample <> 100 OR @CurrentStatisticsSample IS NULL)
        BEGIN
          SET @CurrentStatisticsSample = NULL
          SET @CurrentStatisticsResample = 'Y'
        END

        -- Incremental statistics only supports RESAMPLE
        IF @PartitionLevelStatistics = 1 AND @CurrentIsIncremental = 1
        BEGIN
          SET @CurrentStatisticsSample = NULL
          SET @CurrentStatisticsResample = 'Y'
        END

        -- Create index comment
        IF @CurrentIndexID IS NOT NULL
        BEGIN
          SET @CurrentComment = 'ObjectType: ' + CASE WHEN @CurrentObjectType = 'U' THEN 'Table' WHEN @CurrentObjectType = 'V' THEN 'View' ELSE 'N/A' END + ', '
          SET @CurrentComment += 'IndexType: ' + CASE WHEN @CurrentIndexType = 1 THEN 'Clustered' WHEN @CurrentIndexType = 2 THEN 'NonClustered' WHEN @CurrentIndexType = 3 THEN 'XML' WHEN @CurrentIndexType = 4 THEN 'Spatial' WHEN @CurrentIndexType = 5 THEN 'Clustered Columnstore' WHEN @CurrentIndexType = 6 THEN 'NonClustered Columnstore' WHEN @CurrentIndexType = 7 THEN 'NonClustered Hash' ELSE 'N/A' END + ', '
          SET @CurrentComment += 'ImageText: ' + CASE WHEN @CurrentIsImageText = 1 THEN 'Yes' WHEN @CurrentIsImageText = 0 THEN 'No' ELSE 'N/A' END + ', '
          SET @CurrentComment += 'NewLOB: ' + CASE WHEN @CurrentIsNewLOB = 1 THEN 'Yes' WHEN @CurrentIsNewLOB = 0 THEN 'No' ELSE 'N/A' END + ', '
          SET @CurrentComment += 'FileStream: ' + CASE WHEN @CurrentIsFileStream = 1 THEN 'Yes' WHEN @CurrentIsFileStream = 0 THEN 'No' ELSE 'N/A' END + ', '
          IF @Version >= 11 SET @CurrentComment += 'ColumnStore: ' + CASE WHEN @CurrentIsColumnStore = 1 THEN 'Yes' WHEN @CurrentIsColumnStore = 0 THEN 'No' ELSE 'N/A' END + ', '
          IF @Version >= 14 AND @Resumable = 'Y' SET @CurrentComment += 'Computed: ' + CASE WHEN @CurrentIsComputed = 1 THEN 'Yes' WHEN @CurrentIsComputed = 0 THEN 'No' ELSE 'N/A' END + ', '
          IF @Version >= 14 AND @Resumable = 'Y' SET @CurrentComment += 'Timestamp: ' + CASE WHEN @CurrentIsTimestamp = 1 THEN 'Yes' WHEN @CurrentIsTimestamp = 0 THEN 'No' ELSE 'N/A' END + ', '
          SET @CurrentComment += 'AllowPageLocks: ' + CASE WHEN @CurrentAllowPageLocks = 1 THEN 'Yes' WHEN @CurrentAllowPageLocks = 0 THEN 'No' ELSE 'N/A' END + ', '
          SET @CurrentComment += 'PageCount: ' + ISNULL(CAST(@CurrentPageCount AS nvarchar),'N/A') + ', '
          SET @CurrentComment += 'Fragmentation: ' + ISNULL(CAST(@CurrentFragmentationLevel AS nvarchar),'N/A')
        END

        IF @CurrentIndexID IS NOT NULL AND (@CurrentPageCount IS NOT NULL OR @CurrentFragmentationLevel IS NOT NULL)
        BEGIN
        SET @CurrentExtendedInfo = (SELECT *
                                    FROM (SELECT CAST(@CurrentPageCount AS nvarchar) AS [PageCount],
                                                 CAST(@CurrentFragmentationLevel AS nvarchar) AS Fragmentation
                                    ) ExtendedInfo FOR XML RAW('ExtendedInfo'), ELEMENTS)
        END

        IF @CurrentIndexID IS NOT NULL AND @CurrentAction IS NOT NULL AND (SYSDATETIME() < DATEADD(SECOND,@TimeLimit,@StartTime) OR @TimeLimit IS NULL)
        BEGIN
          SET @CurrentDatabaseContext = @CurrentDatabaseName

          SET @CurrentCommandType = 'ALTER_INDEX'

          SET @CurrentCommand = ''
          IF @LockTimeout IS NOT NULL SET @CurrentCommand = 'SET LOCK_TIMEOUT ' + CAST(@LockTimeout * 1000 AS nvarchar) + '; '
          SET @CurrentCommand += 'ALTER INDEX ' + QUOTENAME(@CurrentIndexName) + ' ON ' + QUOTENAME(@CurrentSchemaName) + '.' + QUOTENAME(@CurrentObjectName)
          IF @CurrentResumableIndexOperation = 1 SET @CurrentCommand += ' RESUME'
          IF @CurrentAction IN('INDEX_REBUILD_ONLINE','INDEX_REBUILD_OFFLINE') AND @CurrentResumableIndexOperation = 0 SET @CurrentCommand += ' REBUILD'
          IF @CurrentAction IN('INDEX_REORGANIZE') AND @CurrentResumableIndexOperation = 0 SET @CurrentCommand += ' REORGANIZE'
          IF @CurrentIsPartition = 1 AND @CurrentResumableIndexOperation = 0 SET @CurrentCommand += ' PARTITION = ' + CAST(@CurrentPartitionNumber AS nvarchar)

          IF @CurrentAction IN('INDEX_REBUILD_ONLINE','INDEX_REBUILD_OFFLINE') AND @SortInTempdb = 'Y' AND @CurrentIndexType IN(1,2,3,4) AND @CurrentResumableIndexOperation = 0
          BEGIN
            INSERT INTO @CurrentAlterIndexWithClauseArguments (Argument)
            SELECT 'SORT_IN_TEMPDB = ON'
          END

          IF @CurrentAction IN('INDEX_REBUILD_ONLINE','INDEX_REBUILD_OFFLINE') AND @SortInTempdb = 'N' AND @CurrentIndexType IN(1,2,3,4) AND @CurrentResumableIndexOperation = 0
          BEGIN
            INSERT INTO @CurrentAlterIndexWithClauseArguments (Argument)
            SELECT 'SORT_IN_TEMPDB = OFF'
          END

          IF @CurrentAction = 'INDEX_REBUILD_ONLINE' AND (@CurrentIsPartition = 0 OR @Version >= 12) AND @CurrentResumableIndexOperation = 0
          BEGIN
            INSERT INTO @CurrentAlterIndexWithClauseArguments (Argument)
            SELECT 'ONLINE = ON' + CASE WHEN @WaitAtLowPriorityMaxDuration IS NOT NULL THEN ' (WAIT_AT_LOW_PRIORITY (MAX_DURATION = ' + CAST(@WaitAtLowPriorityMaxDuration AS nvarchar) + ', ABORT_AFTER_WAIT = ' + UPPER(@WaitAtLowPriorityAbortAfterWait) + '))' ELSE '' END
          END

          IF @CurrentAction = 'INDEX_REBUILD_OFFLINE' AND (@CurrentIsPartition = 0 OR @Version >= 12) AND @CurrentResumableIndexOperation = 0
          BEGIN
            INSERT INTO @CurrentAlterIndexWithClauseArguments (Argument)
            SELECT 'ONLINE = OFF'
          END

          IF @CurrentAction IN('INDEX_REBUILD_ONLINE','INDEX_REBUILD_OFFLINE') AND @CurrentMaxDOP IS NOT NULL
          BEGIN
            INSERT INTO @CurrentAlterIndexWithClauseArguments (Argument)
            SELECT 'MAXDOP = ' + CAST(@CurrentMaxDOP AS nvarchar)
          END

          IF @CurrentAction IN('INDEX_REBUILD_ONLINE','INDEX_REBUILD_OFFLINE') AND @FillFactor IS NOT NULL AND @CurrentIsPartition = 0 AND @CurrentIndexType IN(1,2,3,4) AND @CurrentResumableIndexOperation = 0
          BEGIN
            INSERT INTO @CurrentAlterIndexWithClauseArguments (Argument)
            SELECT 'FILLFACTOR = ' + CAST(@FillFactor AS nvarchar)
          END

          IF @CurrentAction IN('INDEX_REBUILD_ONLINE','INDEX_REBUILD_OFFLINE') AND @PadIndex = 'Y' AND @CurrentIsPartition = 0 AND @CurrentIndexType IN(1,2,3,4) AND @CurrentResumableIndexOperation = 0
          BEGIN
            INSERT INTO @CurrentAlterIndexWithClauseArguments (Argument)
            SELECT 'PAD_INDEX = ON'
          END

          IF (@Version >= 14 OR SERVERPROPERTY('EngineEdition') IN (5,8)) AND @CurrentAction = 'INDEX_REBUILD_ONLINE' AND @CurrentResumableIndexOperation = 0
          BEGIN
            INSERT INTO @CurrentAlterIndexWithClauseArguments (Argument)
            SELECT CASE WHEN @Resumable = 'Y' AND @CurrentIndexType IN(1,2) AND @CurrentIsComputed = 0 AND @CurrentIsTimestamp = 0 THEN 'RESUMABLE = ON' ELSE 'RESUMABLE = OFF' END
          END

          IF (@Version >= 14 OR SERVERPROPERTY('EngineEdition') IN (5,8)) AND @CurrentAction = 'INDEX_REBUILD_ONLINE' AND @CurrentResumableIndexOperation = 0 AND @Resumable = 'Y'  AND @CurrentIndexType IN(1,2) AND @CurrentIsComputed = 0 AND @CurrentIsTimestamp = 0 AND @TimeLimit IS NOT NULL
          BEGIN
            INSERT INTO @CurrentAlterIndexWithClauseArguments (Argument)
            SELECT 'MAX_DURATION = ' + CAST(DATEDIFF(MINUTE,SYSDATETIME(),DATEADD(SECOND,@TimeLimit,@StartTime)) AS nvarchar(max))
          END

          IF @CurrentAction IN('INDEX_REORGANIZE') AND @LOBCompaction = 'Y'
          BEGIN
            INSERT INTO @CurrentAlterIndexWithClauseArguments (Argument)
            SELECT 'LOB_COMPACTION = ON'
          END

          IF @CurrentAction IN('INDEX_REORGANIZE') AND @LOBCompaction = 'N'
          BEGIN
            INSERT INTO @CurrentAlterIndexWithClauseArguments (Argument)
            SELECT 'LOB_COMPACTION = OFF'
          END

          IF EXISTS (SELECT * FROM @CurrentAlterIndexWithClauseArguments)
          BEGIN
            SET @CurrentAlterIndexWithClause = ' WITH ('

            WHILE (1 = 1)
            BEGIN
              SELECT TOP 1 @CurrentAlterIndexArgumentID = ID,
                           @CurrentAlterIndexArgument = Argument
              FROM @CurrentAlterIndexWithClauseArguments
              WHERE Added = 0
              ORDER BY ID ASC

              IF @@ROWCOUNT = 0
              BEGIN
                BREAK
              END

              SET @CurrentAlterIndexWithClause += @CurrentAlterIndexArgument + ', '

              UPDATE @CurrentAlterIndexWithClauseArguments
              SET Added = 1
              WHERE [ID] = @CurrentAlterIndexArgumentID
            END

            SET @CurrentAlterIndexWithClause = RTRIM(@CurrentAlterIndexWithClause)

            SET @CurrentAlterIndexWithClause = LEFT(@CurrentAlterIndexWithClause,LEN(@CurrentAlterIndexWithClause) - 1)

            SET @CurrentAlterIndexWithClause = @CurrentAlterIndexWithClause + ')'
          END

          IF @CurrentAlterIndexWithClause IS NOT NULL SET @CurrentCommand += @CurrentAlterIndexWithClause

          EXECUTE @CurrentCommandOutput = dbo.CommandExecute @DatabaseContext = @CurrentDatabaseName, @Command = @CurrentCommand, @CommandType = @CurrentCommandType, @Mode = 2, @Comment = @CurrentComment, @DatabaseName = @CurrentDatabaseName, @SchemaName = @CurrentSchemaName, @ObjectName = @CurrentObjectName, @ObjectType = @CurrentObjectType, @IndexName = @CurrentIndexName, @IndexType = @CurrentIndexType, @PartitionNumber = @CurrentPartitionNumber, @ExtendedInfo = @CurrentExtendedInfo, @LockMessageSeverity = @LockMessageSeverity, @ExecuteAsUser = @ExecuteAsUser, @LogToTable = @LogToTable, @Execute = @Execute
          SET @Error = @@ERROR
          IF @Error <> 0 SET @CurrentCommandOutput = @Error
          IF @CurrentCommandOutput <> 0 SET @ReturnCode = @CurrentCommandOutput

          IF @Delay > 0
          BEGIN
            SET @CurrentDelay = DATEADD(ss,@Delay,'1900-01-01')
            WAITFOR DELAY @CurrentDelay
          END
        END

        SET @CurrentMaxDOP = @MaxDOP

        -- Create statistics comment
        IF @CurrentStatisticsID IS NOT NULL
        BEGIN
          SET @CurrentComment = 'ObjectType: ' + CASE WHEN @CurrentObjectType = 'U' THEN 'Table' WHEN @CurrentObjectType = 'V' THEN 'View' ELSE 'N/A' END + ', '
          SET @CurrentComment += 'IndexType: ' + CASE WHEN @CurrentIndexID IS NOT NULL THEN 'Index' ELSE 'Column' END + ', '
          IF @CurrentIndexID IS NOT NULL SET @CurrentComment += 'IndexType: ' + CASE WHEN @CurrentIndexType = 1 THEN 'Clustered' WHEN @CurrentIndexType = 2 THEN 'NonClustered' WHEN @CurrentIndexType = 3 THEN 'XML' WHEN @CurrentIndexType = 4 THEN 'Spatial' WHEN @CurrentIndexType = 5 THEN 'Clustered Columnstore' WHEN @CurrentIndexType = 6 THEN 'NonClustered Columnstore' WHEN @CurrentIndexType = 7 THEN 'NonClustered Hash' ELSE 'N/A' END + ', '
          SET @CurrentComment += 'Incremental: ' + CASE WHEN @CurrentIsIncremental = 1 THEN 'Y' WHEN @CurrentIsIncremental = 0 THEN 'N' ELSE 'N/A' END + ', '
          SET @CurrentComment += 'RowCount: ' + ISNULL(CAST(@CurrentRowCount AS nvarchar),'N/A') + ', '
          SET @CurrentComment += 'ModificationCounter: ' + ISNULL(CAST(@CurrentModificationCounter AS nvarchar),'N/A')
        END

        IF @CurrentStatisticsID IS NOT NULL AND (@CurrentRowCount IS NOT NULL OR @CurrentModificationCounter IS NOT NULL)
        BEGIN
        SET @CurrentExtendedInfo = (SELECT *
                                    FROM (SELECT CAST(@CurrentRowCount AS nvarchar) AS [RowCount],
                                                 CAST(@CurrentModificationCounter AS nvarchar) AS ModificationCounter
                                    ) ExtendedInfo FOR XML RAW('ExtendedInfo'), ELEMENTS)
        END

        IF @CurrentStatisticsID IS NOT NULL AND @CurrentUpdateStatistics = 'Y' AND (SYSDATETIME() < DATEADD(SECOND,@TimeLimit,@StartTime) OR @TimeLimit IS NULL)
        BEGIN
          SET @CurrentDatabaseContext = @CurrentDatabaseName

          SET @CurrentCommandType = 'UPDATE_STATISTICS'

          SET @CurrentCommand = ''
          IF @LockTimeout IS NOT NULL SET @CurrentCommand = 'SET LOCK_TIMEOUT ' + CAST(@LockTimeout * 1000 AS nvarchar) + '; '
          SET @CurrentCommand += 'UPDATE STATISTICS ' + QUOTENAME(@CurrentSchemaName) + '.' + QUOTENAME(@CurrentObjectName) + ' ' + QUOTENAME(@CurrentStatisticsName)

          IF @CurrentMaxDOP IS NOT NULL AND ((@Version >= 12.06024 AND @Version < 13) OR (@Version >= 13.05026 AND @Version < 14) OR @Version >= 14.030154)
          BEGIN
            INSERT INTO @CurrentUpdateStatisticsWithClauseArguments (Argument)
            SELECT 'MAXDOP = ' + CAST(@CurrentMaxDOP AS nvarchar)
          END

          IF @CurrentStatisticsSample = 100
          BEGIN
            INSERT INTO @CurrentUpdateStatisticsWithClauseArguments (Argument)
            SELECT 'FULLSCAN'
          END

          IF @CurrentStatisticsSample IS NOT NULL AND @CurrentStatisticsSample <> 100
          BEGIN
            INSERT INTO @CurrentUpdateStatisticsWithClauseArguments (Argument)
            SELECT 'SAMPLE ' + CAST(@CurrentStatisticsSample AS nvarchar) + ' PERCENT'
          END

          IF @CurrentStatisticsResample = 'Y'
          BEGIN
            INSERT INTO @CurrentUpdateStatisticsWithClauseArguments (Argument)
            SELECT 'RESAMPLE'
          END

          IF @CurrentNoRecompute = 1
          BEGIN
            INSERT INTO @CurrentUpdateStatisticsWithClauseArguments (Argument)
            SELECT 'NORECOMPUTE'
          END

          IF EXISTS (SELECT * FROM @CurrentUpdateStatisticsWithClauseArguments)
          BEGIN
            SET @CurrentUpdateStatisticsWithClause = ' WITH'

            WHILE (1 = 1)
            BEGIN
              SELECT TOP 1 @CurrentUpdateStatisticsArgumentID = ID,
                           @CurrentUpdateStatisticsArgument = Argument
              FROM @CurrentUpdateStatisticsWithClauseArguments
              WHERE Added = 0
              ORDER BY ID ASC

              IF @@ROWCOUNT = 0
              BEGIN
                BREAK
              END

              SET @CurrentUpdateStatisticsWithClause = @CurrentUpdateStatisticsWithClause + ' ' + @CurrentUpdateStatisticsArgument + ','

              UPDATE @CurrentUpdateStatisticsWithClauseArguments
              SET Added = 1
              WHERE [ID] = @CurrentUpdateStatisticsArgumentID
            END

            SET @CurrentUpdateStatisticsWithClause = LEFT(@CurrentUpdateStatisticsWithClause,LEN(@CurrentUpdateStatisticsWithClause) - 1)
          END

          IF @CurrentUpdateStatisticsWithClause IS NOT NULL SET @CurrentCommand += @CurrentUpdateStatisticsWithClause

          IF @PartitionLevelStatistics = 1 AND @CurrentIsIncremental = 1 AND @CurrentPartitionNumber IS NOT NULL SET @CurrentCommand += ' ON PARTITIONS(' + CAST(@CurrentPartitionNumber AS nvarchar(max)) + ')'

          EXECUTE @CurrentCommandOutput = dbo.CommandExecute @DatabaseContext = @CurrentDatabaseName, @Command = @CurrentCommand, @CommandType = @CurrentCommandType, @Mode = 2, @Comment = @CurrentComment, @DatabaseName = @CurrentDatabaseName, @SchemaName = @CurrentSchemaName, @ObjectName = @CurrentObjectName, @ObjectType = @CurrentObjectType, @IndexName = @CurrentIndexName, @IndexType = @CurrentIndexType, @StatisticsName = @CurrentStatisticsName, @ExtendedInfo = @CurrentExtendedInfo, @LockMessageSeverity = @LockMessageSeverity, @ExecuteAsUser = @ExecuteAsUser, @LogToTable = @LogToTable, @Execute = @Execute
          SET @Error = @@ERROR
          IF @Error <> 0 SET @CurrentCommandOutput = @Error
          IF @CurrentCommandOutput <> 0 SET @ReturnCode = @CurrentCommandOutput
        END

        NoAction:

        -- Update that the index or statistics is completed
        UPDATE @tmpIndexesStatistics
        SET Completed = 1
        WHERE Selected = 1
        AND Completed = 0
        AND [Order] = @CurrentIxOrder
        AND ID = @CurrentIxID

        -- Clear variables
        SET @CurrentDatabaseContext = NULL

        SET @CurrentCommand = NULL
        SET @CurrentCommandOutput = NULL
        SET @CurrentCommandType = NULL
        SET @CurrentComment = NULL
        SET @CurrentExtendedInfo = NULL

        SET @CurrentIxID = NULL
        SET @CurrentIxOrder = NULL
        SET @CurrentSchemaID = NULL
        SET @CurrentSchemaName = NULL
        SET @CurrentObjectID = NULL
        SET @CurrentObjectName = NULL
        SET @CurrentObjectType = NULL
        SET @CurrentIsMemoryOptimized = NULL
        SET @CurrentIndexID = NULL
        SET @CurrentIndexName = NULL
        SET @CurrentIndexType = NULL
        SET @CurrentStatisticsID = NULL
        SET @CurrentStatisticsName = NULL
        SET @CurrentPartitionID = NULL
        SET @CurrentPartitionNumber = NULL
        SET @CurrentPartitionCount = NULL
        SET @CurrentIsPartition = NULL
        SET @CurrentIndexExists = NULL
        SET @CurrentStatisticsExists = NULL
        SET @CurrentIsImageText = NULL
        SET @CurrentIsNewLOB = NULL
        SET @CurrentIsFileStream = NULL
        SET @CurrentIsColumnStore = NULL
        SET @CurrentIsComputed = NULL
        SET @CurrentIsTimestamp = NULL
        SET @CurrentAllowPageLocks = NULL
        SET @CurrentNoRecompute = NULL
        SET @CurrentIsIncremental = NULL
        SET @CurrentRowCount = NULL
        SET @CurrentModificationCounter = NULL
        SET @CurrentOnReadOnlyFileGroup = NULL
        SET @CurrentResumableIndexOperation = NULL
        SET @CurrentFragmentationLevel = NULL
        SET @CurrentPageCount = NULL
        SET @CurrentFragmentationGroup = NULL
        SET @CurrentAction = NULL
        SET @CurrentMaxDOP = NULL
        SET @CurrentUpdateStatistics = NULL
        SET @CurrentStatisticsSample = NULL
        SET @CurrentStatisticsResample = NULL
        SET @CurrentAlterIndexArgumentID = NULL
        SET @CurrentAlterIndexArgument = NULL
        SET @CurrentAlterIndexWithClause = NULL
        SET @CurrentUpdateStatisticsArgumentID = NULL
        SET @CurrentUpdateStatisticsArgument = NULL
        SET @CurrentUpdateStatisticsWithClause = NULL

        DELETE FROM @CurrentActionsAllowed
        DELETE FROM @CurrentAlterIndexWithClauseArguments
        DELETE FROM @CurrentUpdateStatisticsWithClauseArguments

      END

    END

    IF @CurrentDatabaseState = 'SUSPECT'
    BEGIN
      SET @ErrorMessage = 'The database ' + QUOTENAME(@CurrentDatabaseName) + ' is in a SUSPECT state.'
      RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
      RAISERROR(@EmptyLine,10,1) WITH NOWAIT
      SET @Error = @@ERROR
    END

    -- Update that the database is completed
    IF @DatabasesInParallel = 'Y'
    BEGIN
      UPDATE dbo.QueueDatabase
      SET DatabaseEndTime = SYSDATETIME()
      WHERE QueueID = @QueueID
      AND DatabaseName = @CurrentDatabaseName
    END
    ELSE
    BEGIN
      UPDATE @tmpDatabases
      SET Completed = 1
      WHERE Selected = 1
      AND Completed = 0
      AND ID = @CurrentDBID
    END

    -- Clear variables
    SET @CurrentDBID = NULL
    SET @CurrentDatabaseName = NULL

    SET @CurrentDatabase_sp_executesql = NULL

    SET @CurrentExecuteAsUserExists = NULL
    SET @CurrentUserAccess = NULL
    SET @CurrentIsReadOnly = NULL
    SET @CurrentDatabaseState = NULL
    SET @CurrentInStandby = NULL
    SET @CurrentRecoveryModel = NULL

    SET @CurrentIsDatabaseAccessible = NULL
    SET @CurrentReplicaID = NULL
    SET @CurrentAvailabilityGroupID = NULL
    SET @CurrentAvailabilityGroup = NULL
    SET @CurrentAvailabilityGroupRole = NULL
    SET @CurrentDatabaseMirroringRole = NULL

    SET @CurrentCommand = NULL

    DELETE FROM @tmpIndexesStatistics

  END

  ----------------------------------------------------------------------------------------------------
  --// Log completing information                                                                 //--
  ----------------------------------------------------------------------------------------------------

  Logging:
  SET @EndMessage = 'Date and time: ' + CONVERT(nvarchar,SYSDATETIME(),120)
  RAISERROR('%s',10,1,@EndMessage) WITH NOWAIT

  RAISERROR(@EmptyLine,10,1) WITH NOWAIT

  IF @ReturnCode <> 0
  BEGIN
    RETURN @ReturnCode
  END

  ----------------------------------------------------------------------------------------------------

END

GO

--Ascent
-- added sp_DatabaseRestore from the Brent Ozar First Responder Kit
-- Version = '8.11', VersionDate = '20221013'

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF OBJECT_ID('dbo.sp_DatabaseRestore') IS NULL
	EXEC ('CREATE PROCEDURE dbo.sp_DatabaseRestore AS RETURN 0;');
GO
ALTER PROCEDURE [dbo].[sp_DatabaseRestore]
    @Database NVARCHAR(128) = NULL, 
    @RestoreDatabaseName NVARCHAR(128) = NULL, 
    @BackupPathFull NVARCHAR(260) = NULL, 
    @BackupPathDiff NVARCHAR(260) = NULL, 
    @BackupPathLog NVARCHAR(260) = NULL,
    @MoveFiles BIT = 1, 
    @MoveDataDrive NVARCHAR(260) = NULL, 
    @MoveLogDrive NVARCHAR(260) = NULL, 
    @MoveFilestreamDrive NVARCHAR(260) = NULL,
    @MoveFullTextCatalogDrive NVARCHAR(260) = NULL, 
    @BufferCount INT = NULL,
    @MaxTransferSize INT = NULL,
    @BlockSize INT = NULL,
    @TestRestore BIT = 0, 
    @RunCheckDB BIT = 0, 
    @RestoreDiff BIT = 0,
    @ContinueLogs BIT = 0, 
    @StandbyMode BIT = 0,
    @StandbyUndoPath NVARCHAR(MAX) = NULL,
    @RunRecovery BIT = 0, 
    @ForceSimpleRecovery BIT = 0,
    @ExistingDBAction tinyint = 0,
    @StopAt NVARCHAR(14) = NULL,
    @OnlyLogsAfter NVARCHAR(14) = NULL,
    @SimpleFolderEnumeration BIT = 0,
    @SkipBackupsAlreadyInMsdb BIT = 0,
    @DatabaseOwner sysname = NULL,
    @SetTrustworthyON BIT = 0,
    @FixOrphanUsers BIT = 0,
    @KeepCdc BIT = 0,							
    @Execute CHAR(1) = Y,
    @FileExtensionDiff NVARCHAR(128) = NULL,
    @Debug INT = 0, 
    @Help BIT = 0,
	--Ascent
	@Stats CHAR(1) = Y,
	-- /Ascent
    @Version     VARCHAR(30) = NULL OUTPUT,
	@VersionDate DATETIME = NULL OUTPUT,
    @VersionCheckMode BIT = 0
    @FileNamePrefix NVARCHAR(260) = NULL,
	@RunStoredProcAfterRestore NVARCHAR(260) = NULLAS
SET NOCOUNT ON;
SET STATISTICS XML OFF;
  ----------------------------------------------------------------------------------------------------
  /* Source:  Database Maintenance Solution provided by 
  Ascent Technology PTY (Ltd)
  https://www.ascent.tech
  Version: 2024-06-14                                                               */
  ----------------------------------------------------------------------------------------------------

/*Versioning details*/

SELECT @Version = '8.20', @VersionDate = '20240522';

IF(@VersionCheckMode = 1)
BEGIN
	RETURN;
END;

 
IF @Help = 1
BEGIN
	PRINT '
	/*
		sp_DatabaseRestore from http://FirstResponderKit.org
			
		This script will restore a database from a given file path.
		
		To learn more, visit http://FirstResponderKit.org where you can download new
		versions for free, watch training videos on how it works, get more info on
		the findings, contribute your own code, and more.
		
		Known limitations of this version:
			- Only Microsoft-supported versions of SQL Server. Sorry, 2005 and 2000.
			- Tastes awful with marmite.
		
		Unknown limitations of this version:
			- None.  (If we knew them, they would be known. Duh.)
		
		    Changes - for the full list of improvements and fixes in this version, see:
		    https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/
		
		MIT License
			
		Copyright (c) Brent Ozar Unlimited
		
		Permission is hereby granted, free of charge, to any person obtaining a copy
		of this software and associated documentation files (the "Software"), to deal
		in the Software without restriction, including without limitation the rights
		to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
		copies of the Software, and to permit persons to whom the Software is
		furnished to do so, subject to the following conditions:
		
		The above copyright notice and this permission notice shall be included in all
		copies or substantial portions of the Software.
		
		THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
		IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
		FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
		AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
		LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
		OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
		SOFTWARE.
		
	*/
	';
		
	PRINT '
	/*
	EXEC dbo.sp_DatabaseRestore 
		@Database = ''LogShipMe'', 
		@BackupPathFull = ''D:\Backup\SQL2016PROD1A\LogShipMe\FULL\'', 
		@BackupPathLog = ''D:\Backup\SQL2016PROD1A\LogShipMe\LOG\'', 
		@ContinueLogs = 0, 
		@RunRecovery = 0;
		
	EXEC dbo.sp_DatabaseRestore 
		@Database = ''LogShipMe'', 
		@BackupPathFull = ''D:\Backup\SQL2016PROD1A\LogShipMe\FULL\'', 
		@BackupPathLog = ''D:\Backup\SQL2016PROD1A\LogShipMe\LOG\'', 
		@ContinueLogs = 1, 
		@RunRecovery = 0;
		
	EXEC dbo.sp_DatabaseRestore 
		@Database = ''LogShipMe'', 
		@BackupPathFull = ''D:\Backup\SQL2016PROD1A\LogShipMe\FULL\'', 
		@BackupPathLog = ''D:\Backup\SQL2016PROD1A\LogShipMe\LOG\'', 
		@ContinueLogs = 1, 
		@RunRecovery = 1;
		
	EXEC dbo.sp_DatabaseRestore 
		@Database = ''LogShipMe'', 
		@BackupPathFull = ''D:\Backup\SQL2016PROD1A\LogShipMe\FULL\'', 
		@BackupPathLog = ''D:\Backup\SQL2016PROD1A\LogShipMe\LOG\'', 
		@ContinueLogs = 0, 
		@RunRecovery = 1;
		
	EXEC dbo.sp_DatabaseRestore 
		@Database = ''LogShipMe'', 
		@BackupPathFull = ''D:\Backup\SQL2016PROD1A\LogShipMe\FULL\'', 
		@BackupPathDiff = ''D:\Backup\SQL2016PROD1A\LogShipMe\DIFF\'',
		@BackupPathLog = ''D:\Backup\SQL2016PROD1A\LogShipMe\LOG\'', 
		@RestoreDiff = 1,
		@ContinueLogs = 0, 
		@RunRecovery = 1;
		 
	EXEC dbo.sp_DatabaseRestore 
		@Database = ''LogShipMe'', 
		@BackupPathFull = ''\\StorageServer\LogShipMe\FULL\'', 
		@BackupPathDiff = ''\\StorageServer\LogShipMe\DIFF\'',
		@BackupPathLog = ''\\StorageServer\LogShipMe\LOG\'', 
		@RestoreDiff = 1,
		@ContinueLogs = 0, 
		@RunRecovery = 1,
		@TestRestore = 1,
		@RunCheckDB = 1,
		@Debug = 0;

	EXEC dbo.sp_DatabaseRestore 
		@Database = ''LogShipMe'', 
		@BackupPathFull = ''\\StorageServer\LogShipMe\FULL\'', 
		@BackupPathLog = ''\\StorageServer\LogShipMe\LOG\'',
		@StandbyMode = 1,
		@StandbyUndoPath = ''D:\Data\'',
		@ContinueLogs = 1, 
		@RunRecovery = 0,
		@Debug = 0;

	--Restore just through the latest DIFF, ignoring logs, and using a custom ".dif" file extension
	EXEC dbo.sp_DatabaseRestore 
		@Database = ''LogShipMe'', 
		@BackupPathFull = ''D:\Backup\SQL2016PROD1A\LogShipMe\FULL\'', 
		@BackupPathDiff = ''D:\Backup\SQL2016PROD1A\LogShipMe\DIFF\'',
		@RestoreDiff = 1,
		@FileExtensionDiff = ''dif'',
		@ContinueLogs = 0, 
		@RunRecovery = 1;
	-- Restore from stripped backup set when multiple paths are used. This example will restore stripped full backup set along with stripped transactional logs set from multiple backup paths
	EXEC dbo.sp_DatabaseRestore 
		@Database = ''DBA'', 
		@BackupPathFull = ''D:\Backup1\DBA\FULL,D:\Backup2\DBA\FULL'', 
		@BackupPathLog = ''D:\Backup1\DBA\LOG,D:\Backup2\DBA\LOG'', 
		@StandbyMode = 0,
		@ContinueLogs = 1, 
		@RunRecovery = 0,
		@Debug = 0;
		
	--This example will restore the latest differential backup, and stop transaction logs at the specified date time.  It will execute and print debug information.
	EXEC dbo.sp_DatabaseRestore 
		@Database = ''DBA'', 
		@BackupPathFull = ''\\StorageServer\LogShipMe\FULL\'', 
		@BackupPathDiff = ''\\StorageServer\LogShipMe\DIFF\'',
		@BackupPathLog = ''\\StorageServer\LogShipMe\LOG\'', 
		@RestoreDiff = 1,
		@ContinueLogs = 0, 
		@RunRecovery = 1,
		@StopAt = ''20170508201501'',
		@Debug = 1;

	--This example will NOT execute the restore.  Commands will be printed in a copy/paste ready format only
	EXEC dbo.sp_DatabaseRestore 
		@Database = ''DBA'', 
		@BackupPathFull = ''\\StorageServer\LogShipMe\FULL\'', 
		@BackupPathDiff = ''\\StorageServer\LogShipMe\DIFF\'',
		@BackupPathLog = ''\\StorageServer\LogShipMe\LOG\'', 
		@RestoreDiff = 1,
		@ContinueLogs = 0, 
		@RunRecovery = 1,
		@TestRestore = 1,
		@RunCheckDB = 1,
		@Debug = 0,
		@Execute = ''N'';
	';
	
    RETURN; 
END;

-- Get the SQL Server version number because the columns returned by RESTORE commands vary by version
-- Based on: https://www.brentozar.com/archive/2015/05/sql-server-version-detection/
-- Need to capture BuildVersion because RESTORE HEADERONLY changed with 2014 CU1, not RTM
DECLARE @ProductVersion AS NVARCHAR(20) = CAST(SERVERPROPERTY ('productversion') AS NVARCHAR(20));
DECLARE @MajorVersion AS SMALLINT = CAST(PARSENAME(@ProductVersion, 4) AS SMALLINT);
DECLARE @MinorVersion AS SMALLINT = CAST(PARSENAME(@ProductVersion, 3) AS SMALLINT);
DECLARE @BuildVersion AS SMALLINT = CAST(PARSENAME(@ProductVersion, 2) AS SMALLINT);

IF @MajorVersion < 10
BEGIN
    RAISERROR('Sorry, DatabaseRestore doesn''t work on versions of SQL prior to 2008.', 15, 1);
    RETURN;
END;

BEGIN TRY 
DECLARE @CurrentDatabaseContext AS VARCHAR(128) = (SELECT DB_NAME());
DECLARE @CommandExecuteCheck VARCHAR(315)

SET @CommandExecuteCheck = 'IF NOT EXISTS (SELECT name FROM ' +@CurrentDatabaseContext+'.sys.objects WHERE type = ''P'' AND name = ''CommandExecute'')
BEGIN
	RAISERROR (''DatabaseRestore requires the CommandExecute stored procedure from the OLA Hallengren Maintenance solution, are you using the correct database?'', 15, 1);
	RETURN;
END;'
EXEC (@CommandExecuteCheck)
END TRY
BEGIN CATCH
THROW;
END CATCH

DECLARE @cmd NVARCHAR(4000) = N'', --Holds xp_cmdshell command
        @sql NVARCHAR(MAX) = N'', --Holds executable SQL commands
        @LastFullBackup NVARCHAR(500) = N'', --Last full backup name
        @LastDiffBackup NVARCHAR(500) = N'', --Last diff backup name
        @LastDiffBackupDateTime NVARCHAR(500) = N'', --Last diff backup date
        @BackupFile NVARCHAR(500) = N'', --Name of backup file
        @BackupDateTime AS CHAR(15) = N'', --Used for comparisons to generate ordered backup files/create a stopat point
        @FullLastLSN NUMERIC(25, 0), --LSN for full
        @DiffLastLSN NUMERIC(25, 0), --LSN for diff
		@HeadersSQL AS NVARCHAR(4000) = N'', --Dynamic insert into #Headers table (deals with varying results from RESTORE FILELISTONLY across different versions)
		@MoveOption AS NVARCHAR(MAX) = N'', --If you need to move restored files to a different directory
		@LogRecoveryOption AS NVARCHAR(MAX) = N'', --Holds the option to cause logs to be restored in standby mode or with no recovery
		@DatabaseLastLSN NUMERIC(25, 0), --redo_start_lsn of the current database
		@i TINYINT = 1,  --Maintains loop to continue logs
		@LogRestoreRanking INT = 1, --Holds Log iteration # when multiple paths & backup files are being stripped
		@LogFirstLSN NUMERIC(25, 0), --Holds first LSN in log backup headers
		@LogLastLSN NUMERIC(25, 0), --Holds last LSN in log backup headers
		@LogLastNameInMsdbAS NVARCHAR(MAX) = N'', -- Holds last TRN file name already restored 
		@FileListParamSQL NVARCHAR(4000) = N'', --Holds INSERT list for #FileListParameters
		@BackupParameters NVARCHAR(500) = N'', --Used to save BlockSize, MaxTransferSize and BufferCount
        @RestoreDatabaseID SMALLINT, --Holds DB_ID of @RestoreDatabaseName
		@UnquotedRestoreDatabaseName nvarchar(128);   --Holds the unquoted @RestoreDatabaseName

/* Ascent */

DECLARE @RestoreStats NVARCHAR(50) = N'';

/* /Ascent */

DECLARE @FileListSimple TABLE (
    BackupFile NVARCHAR(255) NOT NULL, 
    depth int NOT NULL, 
    [file] int NOT NULL
);

DECLARE @FileList TABLE (
	BackupPath NVARCHAR(255) NULL,
    BackupFile NVARCHAR(255) NULL
);

DECLARE @PathItem TABLE (
	PathItem NVARCHAR(512)
);


IF OBJECT_ID(N'tempdb..#FileListParameters') IS NOT NULL DROP TABLE #FileListParameters;
CREATE TABLE #FileListParameters
(
    LogicalName NVARCHAR(128) NOT NULL,
    PhysicalName NVARCHAR(260) NOT NULL,
    [Type] CHAR(1) NOT NULL,
    FileGroupName NVARCHAR(120) NULL,
    Size NUMERIC(20, 0) NOT NULL,
    MaxSize NUMERIC(20, 0) NOT NULL,
    FileID BIGINT NULL,
    CreateLSN NUMERIC(25, 0) NULL,
    DropLSN NUMERIC(25, 0) NULL,
    UniqueID UNIQUEIDENTIFIER NULL,
    ReadOnlyLSN NUMERIC(25, 0) NULL,
    ReadWriteLSN NUMERIC(25, 0) NULL,
    BackupSizeInBytes BIGINT NULL,
    SourceBlockSize INT NULL,
    FileGroupID INT NULL,
    LogGroupGUID UNIQUEIDENTIFIER NULL,
    DifferentialBaseLSN NUMERIC(25, 0) NULL,
    DifferentialBaseGUID UNIQUEIDENTIFIER NULL,
    IsReadOnly BIT NULL,
    IsPresent BIT NULL,
    TDEThumbprint VARBINARY(32) NULL,
    SnapshotUrl NVARCHAR(360) NULL
);

IF OBJECT_ID(N'tempdb..#Headers') IS NOT NULL DROP TABLE #Headers;
CREATE TABLE #Headers
(
    BackupName NVARCHAR(256),
    BackupDescription NVARCHAR(256),
    BackupType NVARCHAR(256),
    ExpirationDate NVARCHAR(256),
    Compressed NVARCHAR(256),
    Position NVARCHAR(256),
    DeviceType NVARCHAR(256),
    UserName NVARCHAR(256),
    ServerName NVARCHAR(256),
    DatabaseName NVARCHAR(256),
    DatabaseVersion NVARCHAR(256),
    DatabaseCreationDate NVARCHAR(256),
    BackupSize NVARCHAR(256),
    FirstLSN NVARCHAR(256),
    LastLSN NVARCHAR(256),
    CheckpointLSN NVARCHAR(256),
    DatabaseBackupLSN NVARCHAR(256),
    BackupStartDate NVARCHAR(256),
    BackupFinishDate NVARCHAR(256),
    SortOrder NVARCHAR(256),
    [CodePage] NVARCHAR(256),
    UnicodeLocaleId NVARCHAR(256),
    UnicodeComparisonStyle NVARCHAR(256),
    CompatibilityLevel NVARCHAR(256),
    SoftwareVendorId NVARCHAR(256),
    SoftwareVersionMajor NVARCHAR(256),
    SoftwareVersionMinor NVARCHAR(256),
    SoftwareVersionBuild NVARCHAR(256),
    MachineName NVARCHAR(256),
    Flags NVARCHAR(256),
    BindingID NVARCHAR(256),
    RecoveryForkID NVARCHAR(256),
    Collation NVARCHAR(256),
    FamilyGUID NVARCHAR(256),
    HasBulkLoggedData NVARCHAR(256),
    IsSnapshot NVARCHAR(256),
    IsReadOnly NVARCHAR(256),
    IsSingleUser NVARCHAR(256),
    HasBackupChecksums NVARCHAR(256),
    IsDamaged NVARCHAR(256),
    BeginsLogChain NVARCHAR(256),
    HasIncompleteMetaData NVARCHAR(256),
    IsForceOffline NVARCHAR(256),
    IsCopyOnly NVARCHAR(256),
    FirstRecoveryForkID NVARCHAR(256),
    ForkPointLSN NVARCHAR(256),
    RecoveryModel NVARCHAR(256),
    DifferentialBaseLSN NVARCHAR(256),
    DifferentialBaseGUID NVARCHAR(256),
    BackupTypeDescription NVARCHAR(256),
    BackupSetGUID NVARCHAR(256),
    CompressedBackupSize NVARCHAR(256),
    Containment NVARCHAR(256),
    KeyAlgorithm NVARCHAR(32),
    EncryptorThumbprint VARBINARY(20),
    EncryptorType NVARCHAR(32),
	LastValidRestoreTime DATETIME, 
	TimeZone NVARCHAR(32), 
	CompressionAlgorithm NVARCHAR(32),
    --
    -- Seq added to retain order by
    --
    Seq INT NOT NULL IDENTITY(1, 1)
);

/*
Correct paths in case people forget a final "\" or "/"
*/
/*Full*/
IF (SELECT RIGHT(@BackupPathFull, 1)) <> '/' AND CHARINDEX('/', @BackupPathFull) > 0 --Has to end in a '/'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @BackupPathFull to add a "/"', 0, 1) WITH NOWAIT;
	SET @BackupPathFull += N'/';
END;
ELSE IF (SELECT RIGHT(@BackupPathFull, 1)) <> '\' --Has to end in a '\'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @BackupPathFull to add a "\"', 0, 1) WITH NOWAIT;
	SET @BackupPathFull += N'\';
END;
/*Diff*/
IF (SELECT RIGHT(@BackupPathDiff, 1)) <> '/' AND CHARINDEX('/', @BackupPathDiff) > 0 --Has to end in a '/'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @BackupPathDiff to add a "/"', 0, 1) WITH NOWAIT;
	SET @BackupPathDiff += N'/';
END;
ELSE IF (SELECT RIGHT(@BackupPathDiff, 1)) <> '\' --Has to end in a '\'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @BackupPathDiff to add a "\"', 0, 1) WITH NOWAIT;
	SET @BackupPathDiff += N'\';
END;
/*Log*/
IF (SELECT RIGHT(@BackupPathLog, 1)) <> '/' AND CHARINDEX('/', @BackupPathLog) > 0 --Has to end in a '/'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @BackupPathLog to add a "/"', 0, 1) WITH NOWAIT;
	SET @BackupPathLog += N'/';
END;
IF (SELECT RIGHT(@BackupPathLog, 1)) <> '\' --Has to end in a '\'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @BackupPathLog to add a "\"', 0, 1) WITH NOWAIT;
	SET @BackupPathLog += N'\';
END;
/*Move Data File*/
IF NULLIF(@MoveDataDrive, '') IS NULL
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Getting default data drive for @MoveDataDrive', 0, 1) WITH NOWAIT;
	SET @MoveDataDrive = CAST(SERVERPROPERTY('InstanceDefaultDataPath') AS nvarchar(260));
END;
IF (SELECT RIGHT(@MoveDataDrive, 1)) <> '/' AND CHARINDEX('/', @MoveDataDrive) > 0 --Has to end in a '/'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @MoveDataDrive to add a "/"', 0, 1) WITH NOWAIT;
	SET @MoveDataDrive += N'/';
END;
ELSE IF (SELECT RIGHT(@MoveDataDrive, 1)) <> '\' --Has to end in a '\'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @MoveDataDrive to add a "\"', 0, 1) WITH NOWAIT;
	SET @MoveDataDrive += N'\';
END;
																											 
/*Move Log File*/
IF NULLIF(@MoveLogDrive, '') IS NULL
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Getting default log drive for @MoveLogDrive', 0, 1) WITH NOWAIT;
	SET @MoveLogDrive  = CAST(SERVERPROPERTY('InstanceDefaultLogPath') AS nvarchar(260));
END;
IF (SELECT RIGHT(@MoveLogDrive, 1)) <> '/' AND CHARINDEX('/', @MoveLogDrive) > 0 --Has to end in a '/'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing@MoveLogDrive to add a "/"', 0, 1) WITH NOWAIT;
	SET @MoveLogDrive += N'/';
END;
ELSE IF (SELECT RIGHT(@MoveLogDrive, 1)) <> '\' --Has to end in a '\'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @MoveLogDrive to add a "\"', 0, 1) WITH NOWAIT;
	SET @MoveLogDrive += N'\';
END;
	
/*Move Filestream File*/
IF NULLIF(@MoveFilestreamDrive, '') IS NULL
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Setting default data drive for @MoveFilestreamDrive', 0, 1) WITH NOWAIT;
	SET @MoveFilestreamDrive  = CAST(SERVERPROPERTY('InstanceDefaultDataPath') AS nvarchar(260));
END;
IF (SELECT RIGHT(@MoveFilestreamDrive, 1)) <> '/' AND CHARINDEX('/', @MoveFilestreamDrive) > 0 --Has to end in a '/'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @MoveFilestreamDrive to add a "/"', 0, 1) WITH NOWAIT;
	SET @MoveFilestreamDrive += N'/';
END;
ELSE IF (SELECT RIGHT(@MoveFilestreamDrive, 1)) <> '\' --Has to end in a '\'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @MoveFilestreamDrive to add a "\"', 0, 1) WITH NOWAIT;
	SET @MoveFilestreamDrive += N'\';
END;
																														 
/*Move FullText Catalog File*/
IF NULLIF(@MoveFullTextCatalogDrive, '') IS NULL
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Setting default data drive for @MoveFullTextCatalogDrive', 0, 1) WITH NOWAIT;
	SET @MoveFullTextCatalogDrive  = CAST(SERVERPROPERTY('InstanceDefaultDataPath') AS nvarchar(260));
END;
IF (SELECT RIGHT(@MoveFullTextCatalogDrive, 1)) <> '/' AND CHARINDEX('/', @MoveFullTextCatalogDrive) > 0 --Has to end in a '/'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @MoveFullTextCatalogDrive to add a "/"', 0, 1) WITH NOWAIT;
	SET @MoveFullTextCatalogDrive += N'/';
END;
IF (SELECT RIGHT(@MoveFullTextCatalogDrive, 1)) <> '\' --Has to end in a '\'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @MoveFullTextCatalogDrive to add a "\"', 0, 1) WITH NOWAIT;
	SET @MoveFullTextCatalogDrive += N'\';
END;
/*Standby Undo File*/
IF (SELECT RIGHT(@StandbyUndoPath, 1)) <> '/' AND CHARINDEX('/', @StandbyUndoPath) > 0 --Has to end in a '/'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @StandbyUndoPath to add a "/"', 0, 1) WITH NOWAIT;
	SET @StandbyUndoPath += N'/';
END;
					 
IF (SELECT RIGHT(@StandbyUndoPath, 1)) <> '\' --Has to end in a '\'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @StandbyUndoPath to add a "\"', 0, 1) WITH NOWAIT;
	SET @StandbyUndoPath += N'\';
END;

IF @RestoreDatabaseName IS NULL OR @RestoreDatabaseName LIKE N'' /*use LIKE instead of =, otherwise N'' = N' '. See: https://www.brentozar.com/archive/2017/04/surprising-behavior-trailing-spaces/ */
BEGIN
	SET @RestoreDatabaseName = @Database;
END;

/*check input parameters*/
IF NOT @MaxTransferSize IS NULL
BEGIN
	IF @MaxTransferSize > 4194304
	BEGIN
		RAISERROR('@MaxTransferSize can not be greater then 4194304', 0, 1) WITH NOWAIT;
	END

	IF @MaxTransferSize % 64 <> 0
	BEGIN
		RAISERROR('@MaxTransferSize has to be a multiple of 65536', 0, 1) WITH NOWAIT;
	END
END;

IF NOT @BlockSize IS NULL
BEGIN
	IF @BlockSize NOT IN (512, 1024, 2048, 4096, 8192, 16384, 32768, 65536)
	BEGIN
		RAISERROR('Supported values for @BlockSize are 512, 1024, 2048, 4096, 8192, 16384, 32768, and 65536', 0, 1) WITH NOWAIT;
	END
END

--File Extension cleanup
IF @FileExtensionDiff LIKE '%.%'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Removing "." from @FileExtensionDiff', 0, 1) WITH NOWAIT;
	SET @FileExtensionDiff = REPLACE(@FileExtensionDiff,'.','');
END

SET @RestoreDatabaseID = DB_ID(@RestoreDatabaseName);
SET @RestoreDatabaseName = QUOTENAME(@RestoreDatabaseName);
SET @UnquotedRestoreDatabaseName = PARSENAME(@RestoreDatabaseName,1);

--If xp_cmdshell is disabled, force use of xp_dirtree
IF NOT EXISTS (SELECT * FROM sys.configurations WHERE name = 'xp_cmdshell' AND value_in_use = 1)
    SET @SimpleFolderEnumeration = 1;

SET @HeadersSQL = 
N'INSERT INTO #Headers WITH (TABLOCK)
  (BackupName, BackupDescription, BackupType, ExpirationDate, Compressed, Position, DeviceType, UserName, ServerName
  ,DatabaseName, DatabaseVersion, DatabaseCreationDate, BackupSize, FirstLSN, LastLSN, CheckpointLSN, DatabaseBackupLSN
  ,BackupStartDate, BackupFinishDate, SortOrder, CodePage, UnicodeLocaleId, UnicodeComparisonStyle, CompatibilityLevel
  ,SoftwareVendorId, SoftwareVersionMajor, SoftwareVersionMinor, SoftwareVersionBuild, MachineName, Flags, BindingID
  ,RecoveryForkID, Collation, FamilyGUID, HasBulkLoggedData, IsSnapshot, IsReadOnly, IsSingleUser, HasBackupChecksums
  ,IsDamaged, BeginsLogChain, HasIncompleteMetaData, IsForceOffline, IsCopyOnly, FirstRecoveryForkID, ForkPointLSN
  ,RecoveryModel, DifferentialBaseLSN, DifferentialBaseGUID, BackupTypeDescription, BackupSetGUID, CompressedBackupSize';
  
IF @MajorVersion >= 11
  SET @HeadersSQL += NCHAR(13) + NCHAR(10) + N', Containment';

IF @MajorVersion >= 13 OR (@MajorVersion = 12 AND @BuildVersion >= 2342)
  SET @HeadersSQL += N', KeyAlgorithm, EncryptorThumbprint, EncryptorType';

IF @MajorVersion >= 16
  SET @HeadersSQL += N', LastValidRestoreTime, TimeZone, CompressionAlgorithm';

SET @HeadersSQL += N')' + NCHAR(13) + NCHAR(10);
SET @HeadersSQL += N'EXEC (''RESTORE HEADERONLY FROM DISK=''''{Path}'''''')';

IF @BackupPathFull IS NOT NULL
BEGIN
	DECLARE @CurrentBackupPathFull NVARCHAR(255);

	 -- Split CSV string logic has taken from Ola Hallengren's :) 
	WITH BackupPaths (
	StartPosition, EndPosition, PathItem
	)
	AS (
		   SELECT 1																															   AS StartPosition,
				  ISNULL( NULLIF( CHARINDEX( ',', @BackupPathFull, 1 ), 0 ), LEN( @BackupPathFull ) + 1 )									   AS EndPosition,
				  SUBSTRING( @BackupPathFull, 1, ISNULL( NULLIF( CHARINDEX( ',', @BackupPathFull, 1 ), 0 ), LEN( @BackupPathFull ) + 1 ) - 1 ) AS PathItem
		   WHERE @BackupPathFull IS NOT NULL
		   UNION ALL
		   SELECT CAST( EndPosition AS INT ) + 1																																		 AS StartPosition,
				  ISNULL( NULLIF( CHARINDEX( ',', @BackupPathFull, EndPosition + 1 ), 0 ), LEN( @BackupPathFull ) + 1 )																	 AS EndPosition,
				  SUBSTRING( @BackupPathFull, EndPosition + 1, ISNULL( NULLIF( CHARINDEX( ',', @BackupPathFull, EndPosition + 1 ), 0 ), LEN( @BackupPathFull ) + 1 ) - EndPosition - 1 ) AS PathItem
		   FROM BackupPaths
		   WHERE EndPosition < LEN( @BackupPathFull ) + 1
	 )
	INSERT INTO @PathItem
	SELECT CASE RIGHT( PathItem, 1 ) WHEN '\' THEN PathItem ELSE PathItem + '\' END FROM BackupPaths;

	WHILE 1 = 1
	BEGIN

		SELECT TOP 1 @CurrentBackupPathFull = PathItem FROM @PathItem
		WHERE PathItem > COALESCE( @CurrentBackupPathFull, '' ) ORDER BY PathItem;
		IF @@rowcount = 0 BREAK;

		IF @SimpleFolderEnumeration = 1
		BEGIN    -- Get list of files 
			INSERT INTO @FileListSimple (BackupFile, depth, [file]) EXEC master.sys.xp_dirtree @CurrentBackupPathFull, 1, 1;
			INSERT @FileList (BackupPath,BackupFile) SELECT @CurrentBackupPathFull, BackupFile FROM @FileListSimple;
			DELETE FROM @FileListSimple;
		END
		ELSE
		BEGIN
			SET @cmd = N'DIR /b "' + @CurrentBackupPathFull + N'"';
			IF @Debug = 1
			BEGIN
				IF @cmd IS NULL PRINT '@cmd is NULL for @CurrentBackupPathFull';
				PRINT @cmd;
			END;  
			INSERT INTO @FileList (BackupFile) EXEC master.sys.xp_cmdshell @cmd;
			UPDATE @FileList SET BackupPath = @CurrentBackupPathFull
			WHERE BackupPath IS NULL;
		END;
    
		IF @Debug = 1
		BEGIN
			SELECT BackupPath, BackupFile FROM @FileList;
		END;
		IF @SimpleFolderEnumeration = 1
		BEGIN
			/*Check what we can*/
			IF NOT EXISTS (SELECT * FROM @FileList)
			BEGIN
				RAISERROR('(FULL) No rows were returned for that database in path %s', 16, 1, @CurrentBackupPathFull) WITH NOWAIT;
				RETURN;
			END;
		END
		ELSE
		BEGIN
			/*Full Sanity check folders*/
			IF (
				SELECT COUNT(*) 
				FROM @FileList AS fl 
				WHERE fl.BackupFile = 'The system cannot find the path specified.'
				OR fl.BackupFile = 'File Not Found'
				) = 1
			BEGIN
				RAISERROR('(FULL) No rows or bad value for path %s', 16, 1, @CurrentBackupPathFull) WITH NOWAIT;
				RETURN;
			END;
			IF (
				SELECT COUNT(*) 
				FROM @FileList AS fl 
				WHERE fl.BackupFile = 'Access is denied.'
				) = 1
			BEGIN
				RAISERROR('(FULL) Access is denied to %s', 16, 1, @CurrentBackupPathFull) WITH NOWAIT;
				RETURN;
			END;
			IF (
				SELECT COUNT(*) 
				FROM @FileList AS fl 
				) = 1
				AND 
				(
				SELECT COUNT(*) 
				FROM @FileList AS fl 							
				WHERE fl.BackupFile IS NULL
				) = 1
				BEGIN
					RAISERROR('(FULL) Empty directory %s', 16, 1, @CurrentBackupPathFull) WITH NOWAIT;
					RETURN;
				END
			IF (
				SELECT COUNT(*) 
				FROM @FileList AS fl 
				WHERE fl.BackupFile = 'The user name or password is incorrect.'
				) = 1
				BEGIN
					RAISERROR('(FULL) Incorrect user name or password for %s', 16, 1, @CurrentBackupPathFull) WITH NOWAIT;
					RETURN;
				END;
		END;
	END
	/*End folder sanity check*/

	IF @StopAt IS NOT NULL
		BEGIN
			DELETE
			FROM @FileList
			WHERE BackupFile LIKE N'%[_][0-9].bak'
			AND	BackupFile LIKE N'%' + @Database + N'%'
										  
			AND	(REPLACE( RIGHT( REPLACE( BackupFile, RIGHT( BackupFile, PATINDEX( '%_[0-9][0-9]%', REVERSE( BackupFile ) ) ), '' ), 16 ), '_', '' ) > @StopAt);

			DELETE
			FROM @FileList
			WHERE BackupFile LIKE N'%[_][0-9][0-9].bak'
			AND	BackupFile LIKE N'%' + @Database + N'%'
			AND	(REPLACE( RIGHT( REPLACE( BackupFile, RIGHT( BackupFile, PATINDEX( '%_[0-9][0-9]%', REVERSE( BackupFile ) ) ), '' ), 18 ), '_', '' ) > @StopAt);
		END;
	
    -- Find latest full backup 
    SELECT @LastFullBackup = MAX(BackupFile)
    FROM @FileList
    WHERE BackupFile LIKE N'%.bak'
        AND
        BackupFile LIKE N'%' + @Database + N'%'
	    AND
	    (@StopAt IS NULL OR REPLACE( RIGHT( REPLACE( @LastFullBackup, RIGHT( @LastFullBackup, PATINDEX( '%_[0-9][0-9]%', REVERSE( @LastFullBackup ) ) ), '' ), 16 ), '_', '' ) <= @StopAt);

    /*	To get all backups that belong to the same set we can do two things:
		    1.	RESTORE HEADERONLY of ALL backup files in the folder and look for BackupSetGUID.
			    Backups that belong to the same split will have the same BackupSetGUID.
		    2.	Olla Hallengren's solution appends file index at the end of the name:
			    SQLSERVER1_TEST_DB_FULL_20180703_213211_1.bak
			    SQLSERVER1_TEST_DB_FULL_20180703_213211_2.bak
			    SQLSERVER1_TEST_DB_FULL_20180703_213211_N.bak
			    We can and find all related files with the same timestamp but different index.
			    This option is simpler and requires less changes to this procedure */

    IF @LastFullBackup IS NULL
    BEGIN
        RAISERROR('No backups for "%s" found in "%s"', 16, 1, @Database, @BackupPathFull) WITH NOWAIT;
        RETURN;
    END;

	SELECT BackupPath, BackupFile INTO #SplitFullBackups
	FROM @FileList
	WHERE LEFT( BackupFile, LEN( BackupFile ) - PATINDEX( '%[_]%', REVERSE( BackupFile ) ) ) = LEFT( @LastFullBackup, LEN( @LastFullBackup ) - PATINDEX( '%[_]%', REVERSE( @LastFullBackup ) ) )
	AND PATINDEX( '%[_]%', REVERSE( @LastFullBackup ) ) <= 7 -- there is a 1 or 2 digit index at the end of the string which indicates split backups. Ola only supports up to 64 file split.
	ORDER BY REPLACE( RIGHT( REPLACE( BackupFile, RIGHT( BackupFile, PATINDEX( '%_[0-9][0-9]%', REVERSE( BackupFile ) ) ), '' ), 16 ), '_', '' ) DESC; 

    -- File list can be obtained by running RESTORE FILELISTONLY of any file from the given BackupSet therefore we do not have to cater for split backups when building @FileListParamSQL

    SET @FileListParamSQL = 
      N'INSERT INTO #FileListParameters WITH (TABLOCK)
       (LogicalName, PhysicalName, Type, FileGroupName, Size, MaxSize, FileID, CreateLSN, DropLSN
       ,UniqueID, ReadOnlyLSN, ReadWriteLSN, BackupSizeInBytes, SourceBlockSize, FileGroupID, LogGroupGUID
       ,DifferentialBaseLSN, DifferentialBaseGUID, IsReadOnly, IsPresent, TDEThumbprint';

    IF @MajorVersion >= 13
    BEGIN
	    SET @FileListParamSQL += N', SnapshotUrl';
    END;

    SET @FileListParamSQL += N')' + NCHAR(13) + NCHAR(10);
    SET @FileListParamSQL += N'EXEC (''RESTORE FILELISTONLY FROM DISK=''''{Path}'''''')';

	-- get the TOP record to use in "Restore HeaderOnly/FileListOnly" statement as well as Non-Split Backups Restore Command
	SELECT TOP 1 @CurrentBackupPathFull = BackupPath, @LastFullBackup = BackupFile
	FROM @FileList
	ORDER BY REPLACE( RIGHT( REPLACE( BackupFile, RIGHT( BackupFile, PATINDEX( '%_[0-9][0-9]%', REVERSE( BackupFile ) ) ), '' ), 16 ), '_', '' ) DESC;

    SET @sql = REPLACE(@FileListParamSQL, N'{Path}', @CurrentBackupPathFull + @LastFullBackup);

    IF @Debug = 1
    BEGIN
	    IF @sql IS NULL PRINT '@sql is NULL for INSERT to #FileListParameters: @BackupPathFull + @LastFullBackup';
	    PRINT @sql;
    END;

    EXEC (@sql);
    IF @Debug = 1
    BEGIN
	    SELECT '#FileListParameters' AS table_name, * FROM #FileListParameters;
	    SELECT '@FileList' AS table_name, BackupPath, BackupFile FROM @FileList WHERE BackupFile IS NOT NULL;
    END

    --get the backup completed data so we can apply tlogs from that point forwards                                                   
    SET @sql = REPLACE(@HeadersSQL, N'{Path}', @CurrentBackupPathFull + @LastFullBackup);

    IF @Debug = 1
    BEGIN
	    IF @sql IS NULL PRINT '@sql is NULL for get backup completed data: @BackupPathFull, @LastFullBackup';
	    PRINT @sql;
    END;
    EXECUTE (@sql);
    IF @Debug = 1
    BEGIN
	    SELECT '#Headers' AS table_name, @LastFullBackup AS FullBackupFile, * FROM #Headers
    END;

    --Ensure we are looking at the expected backup, but only if we expect to restore a FULL backups
    IF NOT EXISTS (SELECT * FROM #Headers h WHERE h.DatabaseName = @Database)
    BEGIN
        RAISERROR('Backupfile "%s" does not match @Database parameter "%s"', 16, 1, @LastFullBackup, @Database) WITH NOWAIT;
        RETURN;
    END;

	IF NOT @BufferCount IS NULL
	BEGIN
		SET @BackupParameters += N', BufferCount=' + cast(@BufferCount as NVARCHAR(10))
	END

	IF NOT @MaxTransferSize IS NULL
	BEGIN
		SET @BackupParameters += N', MaxTransferSize=' + cast(@MaxTransferSize as NVARCHAR(7))
	END

	IF NOT @BlockSize IS NULL
	BEGIN
		SET @BackupParameters += N', BlockSize=' + cast(@BlockSize as NVARCHAR(5))
	END

/* Ascent */
	IF @Stats = 'Y'
	BEGIN
		SET @RestoreStats = N', STATS = 5'
	END

/* /Ascent */

    IF @MoveFiles = 1
    BEGIN
	    IF @Execute = 'Y' RAISERROR('@MoveFiles = 1, adjusting paths', 0, 1) WITH NOWAIT;

	    WITH Files
	    AS (
		    SELECT
			    CASE
				    WHEN Type = 'D' THEN @MoveDataDrive
				    WHEN Type = 'L' THEN @MoveLogDrive
				    WHEN Type = 'S' THEN @MoveFilestreamDrive
					WHEN Type = 'F' THEN @MoveFullTextCatalogDrive
			    END + COALESCE(@FileNamePrefix, '') + CASE
                        WHEN @Database = @RestoreDatabaseName THEN REVERSE(LEFT(REVERSE(PhysicalName), CHARINDEX('\', REVERSE(PhysicalName), 1) -1))
					    ELSE REPLACE(REVERSE(LEFT(REVERSE(PhysicalName), CHARINDEX('\', REVERSE(PhysicalName), 1) -1)), @Database, SUBSTRING(@RestoreDatabaseName, 2, LEN(@RestoreDatabaseName) -2))
					    END AS TargetPhysicalName,
                    PhysicalName,
                    LogicalName
		    FROM #FileListParameters)
	    SELECT @MoveOption = @MoveOption + N', MOVE ''' + Files.LogicalName + N''' TO ''' + Files.TargetPhysicalName + ''''
	    FROM Files
        WHERE Files.TargetPhysicalName <> Files.PhysicalName;
		
	    IF @Debug = 1 PRINT @MoveOption
    END;

    /*Process @ExistingDBAction flag */
    IF @ExistingDBAction BETWEEN 1 AND 4
    BEGIN
        IF @RestoreDatabaseID IS NOT NULL
        BEGIN
            IF @ExistingDBAction = 1
            BEGIN
                RAISERROR('Setting single user', 0, 1) WITH NOWAIT;
                SET @sql = N'ALTER DATABASE ' + @RestoreDatabaseName + ' SET SINGLE_USER WITH ROLLBACK IMMEDIATE; ' + NCHAR(13);
                IF @Debug = 1 OR @Execute = 'N'
		        BEGIN
			        IF @sql IS NULL PRINT '@sql is NULL for SINGLE_USER';
			        PRINT @sql;
		        END;
		        IF @Debug IN (0, 1) AND @Execute = 'Y'
				BEGIN
					IF DATABASEPROPERTYEX(@UnquotedRestoreDatabaseName,'STATUS') != 'RESTORING' 
					BEGIN
						EXECUTE @sql = [dbo].[CommandExecute] @DatabaseContext=N'master', @Command = @sql, @CommandType = 'ALTER DATABASE SINGLE_USER', @Mode = 1, @DatabaseName = @UnquotedRestoreDatabaseName, @LogToTable = 'Y', @Execute = 'Y';
					END
					ELSE IF @Debug = 1
					BEGIN
						IF DATABASEPROPERTYEX(@UnquotedRestoreDatabaseName,'STATUS') IS NULL PRINT 'Unable to retrieve STATUS from "' + @UnquotedRestoreDatabaseName + '" database. Skipping setting database to SINGLE_USER';
						ELSE IF DATABASEPROPERTYEX(@UnquotedRestoreDatabaseName,'STATUS') = 'RESTORING' PRINT @UnquotedRestoreDatabaseName + ' database STATUS is RESTORING. Skiping setting database to SINGLE_USER';
			        END
		        END
            END
            IF @ExistingDBAction IN (2, 3)
            BEGIN
                RAISERROR('Killing connections', 0, 1) WITH NOWAIT;
                SET @sql = N'/* Kill connections */' + NCHAR(13);
                SELECT 
                    @sql = @sql + N'KILL ' + CAST(spid as nvarchar(5)) + N';' + NCHAR(13)
                FROM
                    --database_ID was only added to sys.dm_exec_sessions in SQL Server 2012 but we need to support older
                    sys.sysprocesses
                WHERE
                    dbid = @RestoreDatabaseID;
                IF @Debug = 1 OR @Execute = 'N'
		        BEGIN
			        IF @sql IS NULL PRINT '@sql is NULL for Kill connections';
			        PRINT @sql;
		        END;
                IF @Debug IN (0, 1) AND @Execute = 'Y'
			       EXECUTE @sql = [dbo].[CommandExecute] @DatabaseContext=N'master', @Command = @sql, @CommandType = 'KILL CONNECTIONS', @Mode = 1, @DatabaseName = @UnquotedRestoreDatabaseName, @LogToTable = 'Y', @Execute = 'Y';
            END
            IF @ExistingDBAction = 3
            BEGIN
                RAISERROR('Dropping database', 0, 1) WITH NOWAIT;
            
                SET @sql = N'DROP DATABASE ' + @RestoreDatabaseName + NCHAR(13);
                IF @Debug = 1 OR @Execute = 'N'
		        BEGIN
			        IF @sql IS NULL PRINT '@sql is NULL for DROP DATABASE';
			        PRINT @sql;
		        END;
		        IF @Debug IN (0, 1) AND @Execute = 'Y'
			        EXECUTE @sql = [dbo].[CommandExecute] @DatabaseContext=N'master', @Command = @sql, @CommandType = 'DROP DATABASE', @Mode = 1, @DatabaseName = @UnquotedRestoreDatabaseName, @LogToTable = 'Y', @Execute = 'Y';
            END
			IF @ExistingDBAction = 4
			BEGIN
				RAISERROR ('Offlining database', 0, 1) WITH NOWAIT;

				SET @sql = N'ALTER DATABASE ' + @RestoreDatabaseName + SPACE( 1 ) + 'SET OFFLINE WITH ROLLBACK IMMEDIATE';
				IF @Debug = 1 OR @Execute = 'N'
				BEGIN
					IF @sql IS NULL PRINT '@sql is NULL for Offline database';
					PRINT @sql;
				END;
		        IF @Debug IN (0, 1) AND @Execute = 'Y'
				BEGIN
					IF DATABASEPROPERTYEX(@UnquotedRestoreDatabaseName,'STATUS') != 'RESTORING' 
					BEGIN
						EXECUTE @sql = [dbo].[CommandExecute] @DatabaseContext=N'master', @Command = @sql, @CommandType = 'OFFLINE DATABASE', @Mode = 1, @DatabaseName = @UnquotedRestoreDatabaseName, @LogToTable = 'Y', @Execute = 'Y';
					END
					ELSE IF @Debug = 1
					BEGIN
						IF DATABASEPROPERTYEX(@UnquotedRestoreDatabaseName,'STATUS') IS NULL PRINT 'Unable to retrieve STATUS from "' + @UnquotedRestoreDatabaseName + '" database. Skipping setting database OFFLINE';
						ELSE IF DATABASEPROPERTYEX(@UnquotedRestoreDatabaseName,'STATUS') = 'RESTORING' PRINT @UnquotedRestoreDatabaseName + ' database STATUS is RESTORING. Skiping setting database OFFLINE';
			        END
		        END
			END;
        END
        ELSE
            RAISERROR('@ExistingDBAction > 0, but no existing @RestoreDatabaseName', 0, 1) WITH NOWAIT;
    END
    ELSE
        IF @Execute = 'Y' OR @Debug = 1 RAISERROR('@ExistingDBAction %u so do nothing', 0, 1, @ExistingDBAction) WITH NOWAIT;

    IF @ContinueLogs = 0
    BEGIN
	    IF @Execute = 'Y' RAISERROR('@ContinueLogs set to 0', 0, 1) WITH NOWAIT;
	
	    /* now take split backups into account */
	    IF (SELECT COUNT(*) FROM #SplitFullBackups) > 0
        BEGIN
            IF @Debug = 1 RAISERROR('Split backups found', 0, 1) WITH NOWAIT;

			SET @sql = N'RESTORE DATABASE ' + @RestoreDatabaseName + N' FROM '
                       + STUFF(
                             (SELECT CHAR( 10 ) + ',DISK=''' + BackupPath + BackupFile + ''''
							  FROM #SplitFullBackups
							  ORDER BY BackupFile
							  FOR XML PATH ('')),
                             1,
                             2,
                             '') + N' WITH NORECOVERY, REPLACE' + @BackupParameters + @MoveOption
							 /* Ascent */ + @RestoreStats
							 + NCHAR(13) + NCHAR(10);
        END;
	    ELSE
		BEGIN
			SET @sql = N'RESTORE DATABASE ' + @RestoreDatabaseName + N' FROM DISK = ''' + @CurrentBackupPathFull + @LastFullBackup + N''' WITH NORECOVERY, REPLACE' + @BackupParameters + @MoveOption
			/* Ascent */+ @RestoreStats
			+ NCHAR(13) + NCHAR(10);
		END
	    IF (@StandbyMode = 1)
	    BEGIN
		    IF (@StandbyUndoPath IS NULL)
			BEGIN
			    IF @Execute = 'Y' OR @Debug = 1 RAISERROR('The file path of the undo file for standby mode was not specified. The database will not be restored in standby mode.', 0, 1) WITH NOWAIT;
			END
	        ELSE IF (SELECT COUNT(*) FROM #SplitFullBackups) > 0
			BEGIN
				SET @sql = @sql + ', STANDBY = ''' + @StandbyUndoPath + @Database + 'Undo.ldf''' + NCHAR(13) + NCHAR(10);
			END
			ELSE
	        BEGIN
		        SET @sql = N'RESTORE DATABASE ' + @RestoreDatabaseName + N' FROM DISK = ''' + @CurrentBackupPathFull + @LastFullBackup + N''' WITH  REPLACE' + @BackupParameters + @MoveOption + N' , STANDBY = ''' + @StandbyUndoPath + @Database + 'Undo.ldf'''
				/* Ascent */ + @RestoreStats
				+ NCHAR(13) + NCHAR(10);
	        END
        END;
		IF @Debug = 1 OR @Execute = 'N'
		BEGIN
			IF @sql IS NULL PRINT '@sql is NULL for RESTORE DATABASE: @BackupPathFull, @LastFullBackup, @MoveOption';
			PRINT @sql;
		END;
			
		IF @Debug IN (0, 1) AND @Execute = 'Y'
			EXECUTE @sql = [dbo].[CommandExecute] @DatabaseContext=N'master', @Command = @sql, @CommandType = 'RESTORE DATABASE', @Mode = 1, @DatabaseName = @UnquotedRestoreDatabaseName, @LogToTable = 'Y', @Execute = 'Y';

		-- We already loaded #Headers above

	    --setting the @BackupDateTime to a numeric string so that it can be used in comparisons
		SET @BackupDateTime = REPLACE( RIGHT( REPLACE( @LastFullBackup, RIGHT( @LastFullBackup, PATINDEX( '%_[0-9][0-9]%', REVERSE( @LastFullBackup ) ) ), '' ), 16 ), '_', '' );
	    
		SELECT @FullLastLSN = CAST(LastLSN AS NUMERIC(25, 0)) FROM #Headers WHERE BackupType = 1;  
		IF @Debug = 1
		BEGIN
			IF @BackupDateTime IS NULL PRINT '@BackupDateTime is NULL for REPLACE: @LastFullBackup';
			PRINT @BackupDateTime;
		END;                                            
	    
	END;
    ELSE
	BEGIN
		
		SELECT @DatabaseLastLSN = CAST(f.redo_start_lsn AS NUMERIC(25, 0))
		FROM master.sys.databases d
		JOIN master.sys.master_files f ON d.database_id = f.database_id
		WHERE d.name = SUBSTRING(@RestoreDatabaseName, 2, LEN(@RestoreDatabaseName) - 2) AND f.file_id = 1;
	
	END;
END;

IF @BackupPathFull IS NULL AND @ContinueLogs = 1
BEGIN
		
	SELECT @DatabaseLastLSN = CAST(f.redo_start_lsn AS NUMERIC(25, 0))
	FROM master.sys.databases d
	JOIN master.sys.master_files f ON d.database_id = f.database_id
	WHERE d.name = SUBSTRING(@RestoreDatabaseName, 2, LEN(@RestoreDatabaseName) - 2) AND f.file_id = 1;
	
END;

IF @BackupPathDiff IS NOT NULL
BEGIN 
    DELETE FROM @FileList;
    DELETE FROM @FileListSimple;
	DELETE FROM @PathItem;

	DECLARE @CurrentBackupPathDiff NVARCHAR(512);
	-- Split CSV string logic has taken from Ola Hallengren's :) 
	WITH BackupPaths (
	StartPosition, EndPosition, PathItem
	)
	AS (
		   SELECT 1																															   AS StartPosition,
				  ISNULL( NULLIF( CHARINDEX( ',', @BackupPathDiff, 1 ), 0 ), LEN( @BackupPathDiff ) + 1 )									   AS EndPosition,
				  SUBSTRING( @BackupPathDiff, 1, ISNULL( NULLIF( CHARINDEX( ',', @BackupPathDiff, 1 ), 0 ), LEN( @BackupPathDiff ) + 1 ) - 1 ) AS PathItem
		   WHERE @BackupPathDiff IS NOT NULL
		   UNION ALL
		   SELECT CAST( EndPosition AS INT ) + 1																																		 AS StartPosition,
				  ISNULL( NULLIF( CHARINDEX( ',', @BackupPathDiff, EndPosition + 1 ), 0 ), LEN( @BackupPathDiff ) + 1 )																	 AS EndPosition,
				  SUBSTRING( @BackupPathDiff, EndPosition + 1, ISNULL( NULLIF( CHARINDEX( ',', @BackupPathDiff, EndPosition + 1 ), 0 ), LEN( @BackupPathDiff ) + 1 ) - EndPosition - 1 ) AS PathItem
		   FROM BackupPaths
		   WHERE EndPosition < LEN( @BackupPathDiff ) + 1
	 )
	INSERT INTO @PathItem
	SELECT CASE RIGHT( PathItem, 1 ) WHEN '\' THEN PathItem ELSE PathItem + '\' END FROM BackupPaths;

	WHILE 1 = 1
	BEGIN

		SELECT TOP 1 @CurrentBackupPathDiff = PathItem FROM @PathItem
		WHERE PathItem > COALESCE( @CurrentBackupPathDiff, '' ) ORDER BY PathItem;
		IF @@rowcount = 0 BREAK;

		IF @SimpleFolderEnumeration = 1
		BEGIN    -- Get list of files 
			INSERT INTO @FileListSimple (BackupFile, depth, [file]) EXEC master.sys.xp_dirtree @CurrentBackupPathDiff, 1, 1;
			INSERT @FileList (BackupPath,BackupFile) SELECT @CurrentBackupPathDiff, BackupFile FROM @FileListSimple;
			DELETE FROM @FileListSimple;
		END
		ELSE
		BEGIN
			SET @cmd = N'DIR /b "' + @CurrentBackupPathDiff + N'"';
			IF @Debug = 1
			BEGIN
				IF @cmd IS NULL PRINT '@cmd is NULL for @CurrentBackupPathDiff';
				PRINT @cmd;
			END;  
			INSERT INTO @FileList (BackupFile) EXEC master.sys.xp_cmdshell @cmd;
			UPDATE @FileList SET BackupPath = @CurrentBackupPathDiff WHERE BackupPath IS NULL;
		END;
    
		IF @Debug = 1
		BEGIN
			SELECT BackupPath,BackupFile FROM @FileList WHERE BackupFile IS NOT NULL;
		END;
		IF @SimpleFolderEnumeration = 0
    BEGIN
        /*Full Sanity check folders*/
        IF (
		    SELECT COUNT(*) 
		    FROM @FileList AS fl 
		    WHERE fl.BackupFile = 'The system cannot find the path specified.'
		    ) = 1
	    BEGIN
		    RAISERROR('(DIFF) Bad value for path %s', 16, 1, @CurrentBackupPathDiff) WITH NOWAIT;
            RETURN;
	    END;
	    IF (
		    SELECT COUNT(*) 
		    FROM @FileList AS fl 
		    WHERE fl.BackupFile = 'Access is denied.'
		    ) = 1
	    BEGIN
		    RAISERROR('(DIFF) Access is denied to %s', 16, 1, @CurrentBackupPathDiff) WITH NOWAIT;
            RETURN;
	    END;
	    IF (
		    SELECT COUNT(*) 
		    FROM @FileList AS fl 
		    WHERE fl.BackupFile = 'The user name or password is incorrect.'
		    ) = 1
		    BEGIN
			    RAISERROR('(DIFF) Incorrect user name or password for %s', 16, 1, @CurrentBackupPathDiff) WITH NOWAIT;
                RETURN;
		    END;
    END;
	END
    /*End folder sanity check*/
    -- Find latest diff backup 
	IF @FileExtensionDiff IS NULL
	BEGIN
		IF @Execute = 'Y' OR @Debug = 1 RAISERROR('No @FileExtensionDiff given, assuming "bak".', 0, 1) WITH NOWAIT;
		SET @FileExtensionDiff = 'bak';
	END

	SELECT TOP 1 @LastDiffBackup = BackupFile, @CurrentBackupPathDiff = BackupPath
    FROM @FileList
    WHERE BackupFile LIKE N'%.' + @FileExtensionDiff
        AND
        BackupFile LIKE N'%' + @Database + '%'
	    AND
	    (@StopAt IS NULL OR REPLACE( RIGHT( REPLACE( BackupFile, RIGHT( BackupFile, PATINDEX( '%_[0-9][0-9]%', REVERSE( BackupFile ) ) ), '' ), 16 ), '_', '' ) <= @StopAt)
	ORDER BY BackupFile DESC;

	 -- Load FileList data into Temp Table sorted by DateTime Stamp desc 
	 SELECT BackupPath, BackupFile INTO #SplitDiffBackups
	 FROM @FileList
	 WHERE LEFT( BackupFile, LEN( BackupFile ) - PATINDEX( '%[_]%', REVERSE( BackupFile ) ) ) = LEFT( @LastDiffBackup, LEN( @LastDiffBackup ) - PATINDEX( '%[_]%', REVERSE( @LastDiffBackup ) ) )
	 AND PATINDEX( '%[_]%', REVERSE( @LastDiffBackup ) ) <= 7 -- there is a 1 or 2 digit index at the end of the string which indicates split backups. Olla only supports up to 64 file split.
	 ORDER BY REPLACE( RIGHT( REPLACE( BackupFile, RIGHT( BackupFile, PATINDEX( '%_[0-9][0-9]%', REVERSE( BackupFile ) ) ), '' ), 16 ), '_', '' ) DESC;

    --No file = no backup to restore
	SET @LastDiffBackupDateTime = REPLACE( RIGHT( REPLACE( @LastDiffBackup, RIGHT( @LastDiffBackup, PATINDEX( '%_[0-9][0-9]%', REVERSE( @LastDiffBackup ) ) ), '' ), 16 ), '_', '' );

    IF @RestoreDiff = 1 AND @BackupDateTime < @LastDiffBackupDateTime
	BEGIN

	    IF (SELECT COUNT(*) FROM #SplitDiffBackups) > 0
		BEGIN
			IF @Debug = 1 RAISERROR ('Split backups found', 0, 1) WITH NOWAIT;
			SET @sql = N'RESTORE DATABASE ' + @RestoreDatabaseName + N' FROM '
									   + STUFF(
											 (SELECT CHAR( 10 ) + ',DISK=''' + BackupPath + BackupFile + ''''
											 FROM #SplitDiffBackups
											 ORDER BY BackupFile
											 FOR XML PATH ('')),
									   1,
									   2,
									   '' ) + N' WITH NORECOVERY, REPLACE' +  @BackupParameters + @MoveOption /* Ascent */ + @RestoreStats + NCHAR(13) + NCHAR(10);
		END;
		ELSE
			SET @sql = N'RESTORE DATABASE ' + @RestoreDatabaseName + N' FROM DISK = ''' + @CurrentBackupPathDiff + @LastDiffBackup + N''' WITH NORECOVERY' + @BackupParameters + @MoveOption /* Ascent */ + @RestoreStats + NCHAR(13) + NCHAR(10);

	    IF (@StandbyMode = 1)
		BEGIN
		    IF (@StandbyUndoPath IS NULL)
			    BEGIN
				    IF @Execute = 'Y' OR @Debug = 1 RAISERROR('The file path of the undo file for standby mode was not specified. The database will not be restored in standby mode.', 0, 1) WITH NOWAIT;
			    END
		    ELSE IF (SELECT COUNT(*) FROM #SplitDiffBackups) > 0
				SET @sql = @sql + ', STANDBY = ''' + @StandbyUndoPath + @Database + 'Undo.ldf''' /* Ascent */ + @RestoreStats + NCHAR(13) + NCHAR(10);
			ELSE 
			    SET @sql = N'RESTORE DATABASE ' + @RestoreDatabaseName + N' FROM DISK = ''' + @BackupPathDiff + @LastDiffBackup + N''' WITH STANDBY = ''' + @StandbyUndoPath + @Database + 'Undo.ldf''' + @BackupParameters + @MoveOption /* Ascent */ + @RestoreStats + NCHAR(13) + NCHAR(10);
	    END;
		IF @Debug = 1 OR @Execute = 'N'
		BEGIN
			IF @sql IS NULL PRINT '@sql is NULL for RESTORE DATABASE: @BackupPathDiff, @LastDiffBackup';
			PRINT @sql;
		END;  
		IF @Debug IN (0, 1) AND @Execute = 'Y'
			EXECUTE @sql = [dbo].[CommandExecute] @DatabaseContext=N'master', @Command = @sql, @CommandType = 'RESTORE DATABASE', @Mode = 1, @DatabaseName = @UnquotedRestoreDatabaseName, @LogToTable = 'Y', @Execute = 'Y';
		
		--get the backup completed data so we can apply tlogs from that point forwards                                                   
		SET @sql = REPLACE(@HeadersSQL, N'{Path}', @CurrentBackupPathDiff + @LastDiffBackup);
		
		IF @Debug = 1
		BEGIN
			IF @sql IS NULL PRINT '@sql is NULL for REPLACE: @CurrentBackupPathDiff, @LastDiffBackup';
			PRINT @sql;
		END;  
		
		EXECUTE (@sql);
		IF @Debug = 1
		BEGIN
			SELECT '#Headers' AS table_name, @LastDiffBackup AS DiffbackupFile, * FROM #Headers AS h WHERE h.BackupType = 5;
		END
		
		--set the @BackupDateTime to the date time on the most recent differential	
		SET @BackupDateTime = ISNULL( @LastDiffBackupDateTime, @BackupDateTime );
		IF @Debug = 1
		BEGIN
			IF @BackupDateTime IS NULL PRINT '@BackupDateTime is NULL for REPLACE: @LastDiffBackupDateTime';
			PRINT @BackupDateTime;
		END; 
		SELECT @DiffLastLSN = CAST(LastLSN AS NUMERIC(25, 0))
		FROM #Headers 
		WHERE BackupType = 5;                                                  
	END;

	IF @DiffLastLSN IS NULL
	BEGIN
		SET @DiffLastLSN=@FullLastLSN
	END
END      


IF @BackupPathLog IS NOT NULL
BEGIN
    DELETE FROM @FileList;
    DELETE FROM @FileListSimple;
	DELETE FROM @PathItem;

	DECLARE @CurrentBackupPathLog NVARCHAR(512);
	-- Split CSV string logic has taken from Ola Hallengren's :) 
	WITH BackupPaths (
	StartPosition, EndPosition, PathItem
	)
	AS (
		   SELECT 1																															 AS StartPosition,
				  ISNULL( NULLIF( CHARINDEX( ',', @BackupPathLog, 1 ), 0 ), LEN( @BackupPathLog ) + 1 )									     AS EndPosition,
				  SUBSTRING( @BackupPathLog, 1, ISNULL( NULLIF( CHARINDEX( ',', @BackupPathLog, 1 ), 0 ), LEN( @BackupPathLog ) + 1 ) - 1 )  AS PathItem
		   WHERE @BackupPathLog IS NOT NULL
		   UNION ALL
		   SELECT CAST( EndPosition AS INT ) + 1																																		 AS StartPosition,
				  ISNULL( NULLIF( CHARINDEX( ',', @BackupPathLog, EndPosition + 1 ), 0 ), LEN( @BackupPathLog ) + 1 )																	 AS EndPosition,
				  SUBSTRING( @BackupPathLog, EndPosition + 1, ISNULL( NULLIF( CHARINDEX( ',', @BackupPathLog, EndPosition + 1 ), 0 ), LEN( @BackupPathLog ) + 1 ) - EndPosition - 1 )    AS PathItem
		   FROM BackupPaths
		   WHERE EndPosition < LEN( @BackupPathLog ) + 1
	 )
	INSERT INTO @PathItem
	SELECT CASE RIGHT( PathItem, 1 ) WHEN '\' THEN PathItem ELSE PathItem + '\' END FROM BackupPaths;

	WHILE 1 = 1
	BEGIN
		SELECT TOP 1 @CurrentBackupPathLog = PathItem FROM @PathItem
		WHERE PathItem > COALESCE( @CurrentBackupPathLog, '' ) ORDER BY PathItem;
		IF @@rowcount = 0 BREAK;

		IF @SimpleFolderEnumeration = 1
		BEGIN    -- Get list of files 
			INSERT INTO @FileListSimple (BackupFile, depth, [file]) EXEC master.sys.xp_dirtree @BackupPathLog, 1, 1;
			INSERT @FileList (BackupPath, BackupFile) SELECT @CurrentBackupPathLog, BackupFile FROM @FileListSimple;
			DELETE FROM @FileListSimple;
		END
		ELSE
		BEGIN
			SET @cmd = N'DIR /b "' + @CurrentBackupPathLog + N'"';
			IF @Debug = 1
			BEGIN
				IF @cmd IS NULL PRINT '@cmd is NULL for @CurrentBackupPathLog';
				PRINT @cmd;
			END;  
			INSERT INTO @FileList (BackupFile) EXEC master.sys.xp_cmdshell @cmd;
			UPDATE @FileList SET BackupPath = @CurrentBackupPathLog
			WHERE BackupPath IS NULL;
		END;
    
		IF @SimpleFolderEnumeration = 1
		BEGIN
			/*Check what we can*/
			IF NOT EXISTS (SELECT * FROM @FileList)
			BEGIN
				RAISERROR('(LOG) No rows were returned for that database %s in path %s', 16, 1, @Database, @CurrentBackupPathLog) WITH NOWAIT;
				RETURN;
			END;
		END
		ELSE
		BEGIN
			/*Full Sanity check folders*/
			IF (
				SELECT COUNT(*) 
				FROM @FileList AS fl 
				WHERE fl.BackupFile = 'The system cannot find the path specified.'
				OR fl.BackupFile = 'File Not Found'
				) = 1
			BEGIN
				RAISERROR('(LOG) No rows or bad value for path %s', 16, 1, @CurrentBackupPathLog) WITH NOWAIT;
				RETURN;
			END;

			IF (
				SELECT COUNT(*) 
				FROM @FileList AS fl 
				WHERE fl.BackupFile = 'Access is denied.'
				) = 1
			BEGIN
				RAISERROR('(LOG) Access is denied to %s', 16, 1, @CurrentBackupPathLog) WITH NOWAIT;
				RETURN;
			END;

			IF (
				SELECT COUNT(*) 
				FROM @FileList AS fl 
				) = 1
				AND 
				(
				SELECT COUNT(*) 
				FROM @FileList AS fl 							
				WHERE fl.BackupFile IS NULL
				) = 1
				BEGIN
					RAISERROR('(LOG) Empty directory %s', 16, 1, @CurrentBackupPathLog) WITH NOWAIT;
					RETURN;
				END

			IF (
				SELECT COUNT(*) 
				FROM @FileList AS fl 
				WHERE fl.BackupFile = 'The user name or password is incorrect.'
				) = 1
				BEGIN
					RAISERROR('(LOG) Incorrect user name or password for %s', 16, 1, @CurrentBackupPathLog) WITH NOWAIT;
					RETURN;
				END;
		END;
	END 
    /*End folder sanity check*/

IF @Debug = 1
BEGIN 
	SELECT * FROM @FileList WHERE BackupFile IS NOT NULL;
END

IF @SkipBackupsAlreadyInMsdb = 1
BEGIN

	SELECT TOP 1 @LogLastNameInMsdbAS = bf.physical_device_name
		FROM msdb.dbo.backupmediafamily bf
		INNER JOIN msdb.dbo.backupset bs ON bs.media_set_id = bf.media_set_id
		INNER JOIN msdb.dbo.restorehistory rh ON rh.backup_set_id = bs.backup_set_id
		WHERE physical_device_name like @BackupPathLog + '%'
		AND rh.destination_database_name = @UnquotedRestoreDatabaseName
		ORDER BY physical_device_name DESC
	
	IF @Debug = 1
	BEGIN
		SELECT 'Keeping LOG backups with name > : ' + @LogLastNameInMsdbAS
	END
	
	DELETE fl
	FROM @FileList AS fl
	WHERE fl.BackupPath + fl.BackupFile <= @LogLastNameInMsdbAS
END



IF (@OnlyLogsAfter IS NOT NULL)
BEGIN
	
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('@OnlyLogsAfter is NOT NULL, deleting from @FileList', 0, 1) WITH NOWAIT;
	
    DELETE fl
	FROM @FileList AS fl
	WHERE BackupFile LIKE N'%.trn'
	AND BackupFile LIKE N'%' + @Database + N'%' 
	AND REPLACE( RIGHT( REPLACE( fl.BackupFile, RIGHT( fl.BackupFile, PATINDEX( '%_[0-9][0-9]%', REVERSE( fl.BackupFile ) ) ), '' ), 16 ), '_', '' ) < @OnlyLogsAfter;
	
END

-- Check for log backups
IF(@BackupDateTime IS NOT NULL AND @BackupDateTime <> '')
	BEGIN 
		DELETE FROM @FileList
		WHERE BackupFile LIKE N'%.trn'
		AND BackupFile LIKE N'%' + @Database + N'%'
		AND NOT (@ContinueLogs = 1 OR (@ContinueLogs = 0 AND REPLACE( RIGHT( REPLACE( BackupFile, RIGHT( BackupFile, PATINDEX( '%_[0-9][0-9]%', REVERSE( BackupFile ) ) ), '' ), 16 ), '_', '' ) >= @BackupDateTime));
	END;


IF (@StandbyMode = 1)
	BEGIN
		IF (@StandbyUndoPath IS NULL)
			BEGIN
				IF @Execute = 'Y' OR @Debug = 1 RAISERROR('The file path of the undo file for standby mode was not specified. Logs will not be restored in standby mode.', 0, 1) WITH NOWAIT;
			END;
		ELSE
			SET @LogRecoveryOption = N'STANDBY = ''' + @StandbyUndoPath + @Database + 'Undo.ldf''';
	END;

IF (@LogRecoveryOption = N'')
	BEGIN
		SET @LogRecoveryOption = N'NORECOVERY';
	END;

IF (@StopAt IS NOT NULL)
BEGIN
	
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('@StopAt is NOT NULL, deleting from @FileList', 0, 1) WITH NOWAIT;

	IF LEN(@StopAt) <> 14 OR PATINDEX('%[^0-9]%', @StopAt) > 0
	BEGIN
		RAISERROR('@StopAt parameter is incorrect. It should contain exactly 14 digits in the format yyyyMMddhhmmss.', 16, 1) WITH NOWAIT;
		RETURN
	END

	IF ISDATE(STUFF(STUFF(STUFF(@StopAt, 13, 0, ':'), 11, 0, ':'), 9, 0, ' ')) = 0
	BEGIN
		RAISERROR('@StopAt is not a valid datetime.', 16, 1) WITH NOWAIT;
		RETURN
	END

	-- Add the STOPAT parameter to the log recovery options but change the value to a valid DATETIME, e.g. '20211118040230' -> '20211118 04:02:30'
	SET @LogRecoveryOption += ', STOPAT = ''' + STUFF(STUFF(STUFF(@StopAt, 13, 0, ':'), 11, 0, ':'), 9, 0, ' ') + ''''

	IF @BackupDateTime = @StopAt
	BEGIN
		IF @Debug = 1 
		BEGIN
			RAISERROR('@StopAt is the end time of a FULL backup, no log files will be restored.', 0, 1) WITH NOWAIT;
		END
	END
	ELSE
	BEGIN 
		DECLARE @ExtraLogFile NVARCHAR(255)
		SELECT TOP 1 @ExtraLogFile = fl.BackupFile
		FROM @FileList AS fl
		WHERE BackupFile LIKE N'%.trn'
		AND BackupFile LIKE N'%' + @Database + N'%' 
		AND REPLACE( RIGHT( REPLACE( fl.BackupFile, RIGHT( fl.BackupFile, PATINDEX( '%_[0-9][0-9]%', REVERSE( fl.BackupFile ) ) ), '' ), 16 ), '_', '' ) > @StopAt
		ORDER BY BackupFile;
	END
	
	IF @ExtraLogFile IS NULL
	BEGIN
		DELETE fl
		FROM @FileList AS fl
		WHERE BackupFile LIKE N'%.trn'
		AND BackupFile LIKE N'%' + @Database + N'%'
		AND REPLACE( RIGHT( REPLACE( fl.BackupFile, RIGHT( fl.BackupFile, PATINDEX( '%_[0-9][0-9]%', REVERSE( fl.BackupFile ) ) ), '' ), 16 ), '_', '' ) > @StopAt;
	END
	ELSE
	BEGIN
		-- If this is a split backup, @ExtraLogFile contains only the first split backup file, either _1.trn or _01.trn
		-- Change @ExtraLogFile to the max split backup file, then delete all log files greater than this
		SET @ExtraLogFile = REPLACE(REPLACE(@ExtraLogFile, '_1.trn', '_9.trn'), '_01.trn', '_64.trn')
		
		DELETE fl
		FROM @FileList AS fl
		WHERE BackupFile LIKE N'%.trn'
		AND BackupFile LIKE N'%' + @Database + N'%' 
		AND fl.BackupFile >	@ExtraLogFile
	END
END

 -- Group Ordering based on Backup File Name excluding Index {#} to construct coma separated string in "Restore Log" Command
SELECT BackupPath,BackupFile,DENSE_RANK() OVER (ORDER BY REPLACE( RIGHT( REPLACE( BackupFile, RIGHT( BackupFile, PATINDEX( '%_[0-9][0-9]%', REVERSE( BackupFile ) ) ), '' ), 16 ), '_', '' )) AS DenseRank INTO #SplitLogBackups
FROM @FileList
WHERE BackupFile IS NOT NULL;

-- Loop through all the files for the database  
	WHILE 1 = 1
		BEGIN

			-- Get the TOP record to use in "Restore HeaderOnly/FileListOnly" statement   
			SELECT TOP 1 @CurrentBackupPathLog = BackupPath, @BackupFile = BackupFile FROM #SplitLogBackups WHERE DenseRank = @LogRestoreRanking;
			IF @@rowcount = 0 BREAK;

			IF @i = 1
			
			BEGIN
		    SET @sql = REPLACE(@HeadersSQL, N'{Path}', @CurrentBackupPathLog + @BackupFile);
			
				IF @Debug = 1
				BEGIN
					IF @sql IS NULL PRINT '@sql is NULL for REPLACE: @HeadersSQL, @CurrentBackupPathLog, @BackupFile';
					PRINT @sql;
				END; 
			
			EXECUTE (@sql);
				
			SELECT TOP 1 @LogFirstLSN = CAST(FirstLSN AS NUMERIC(25, 0)), 
				   @LogLastLSN = CAST(LastLSN AS NUMERIC(25, 0)) 
			FROM #Headers 
			WHERE BackupType = 2;
		
			IF (@ContinueLogs = 0 AND @LogFirstLSN <= @FullLastLSN AND @FullLastLSN <= @LogLastLSN AND @RestoreDiff = 0) OR (@ContinueLogs = 1 AND @LogFirstLSN <= @DatabaseLastLSN AND @DatabaseLastLSN < @LogLastLSN AND @RestoreDiff = 0)
				SET @i = 2;
			
			IF (@ContinueLogs = 0 AND @LogFirstLSN <= @DiffLastLSN AND @DiffLastLSN <= @LogLastLSN AND @RestoreDiff = 1) OR (@ContinueLogs = 1 AND @LogFirstLSN <= @DatabaseLastLSN AND @DatabaseLastLSN < @LogLastLSN AND @RestoreDiff = 1)
				SET @i = 2;
				
			DELETE FROM #Headers WHERE BackupType = 2;


			END;

			IF @i = 1
				BEGIN
					IF @Debug = 1 RAISERROR('No Log to Restore', 0, 1) WITH NOWAIT;
				END

			IF @i = 2
			BEGIN
				IF @Execute = 'Y' RAISERROR('@i set to 2, restoring logs', 0, 1) WITH NOWAIT;

				IF (SELECT COUNT( * ) FROM #SplitLogBackups WHERE DenseRank = @LogRestoreRanking) > 1
				BEGIN
					IF @Debug = 1 RAISERROR ('Split backups found', 0, 1) WITH NOWAIT;
					SET @sql = N'RESTORE LOG ' + @RestoreDatabaseName + N' FROM '
							   + STUFF(
									(SELECT CHAR( 10 ) + ',DISK=''' + BackupPath + BackupFile + ''''
									 FROM #SplitLogBackups 
									 WHERE DenseRank = @LogRestoreRanking
									 ORDER BY BackupFile
									 FOR XML PATH ('')),
								1,
								2,
								'' ) + N' WITH ' + @LogRecoveryOption /* Ascent */ + @RestoreStats + NCHAR(13) + NCHAR(10);
				END;
				ELSE
				SET @sql = N'RESTORE LOG ' + @RestoreDatabaseName + N' FROM DISK = ''' + @CurrentBackupPathLog + @BackupFile + N''' WITH ' + @LogRecoveryOption /* Ascent */ + @RestoreStats + NCHAR(13) + NCHAR(10);
				
					IF @Debug = 1 OR @Execute = 'N'
					BEGIN
						IF @sql IS NULL PRINT '@sql is NULL for RESTORE LOG: @RestoreDatabaseName, @CurrentBackupPathLog, @BackupFile';
						PRINT @sql;
					END; 
				
					IF @Debug IN (0, 1) AND @Execute = 'Y'
						EXECUTE @sql = [dbo].[CommandExecute] @DatabaseContext=N'master', @Command = @sql, @CommandType = 'RESTORE LOG', @Mode = 1, @DatabaseName = @UnquotedRestoreDatabaseName, @LogToTable = 'Y', @Execute = 'Y';
			END;
			
			SET @LogRestoreRanking += 1;
		END;


	IF @Debug = 1
	BEGIN
	    SELECT '#SplitLogBackups' AS table_name, BackupPath, BackupFile FROM #SplitLogBackups;
	END
END

-- Put database in a useable state 
IF @RunRecovery = 1
	BEGIN
		SET @sql = N'RESTORE DATABASE ' + @RestoreDatabaseName + N' WITH RECOVERY';
		
		IF @KeepCdc = 1
			SET @sql = @sql + N', KEEP_CDC';

		SET @sql = @sql + NCHAR(13);

		IF @Debug = 1 OR @Execute = 'N'
		BEGIN
			IF @sql IS NULL PRINT '@sql is NULL for RESTORE DATABASE: @RestoreDatabaseName';
			PRINT @sql;
		END; 

		IF @Debug IN (0, 1) AND @Execute = 'Y'
			EXECUTE @sql = [dbo].[CommandExecute] @DatabaseContext=N'master', @Command = @sql, @CommandType = 'RECOVER DATABASE', @Mode = 1, @DatabaseName = @UnquotedRestoreDatabaseName, @LogToTable = 'Y', @Execute = 'Y';
	END;

-- Ensure simple recovery model
IF @ForceSimpleRecovery = 1
	BEGIN
		SET @sql = N'ALTER DATABASE ' + @RestoreDatabaseName + N' SET RECOVERY SIMPLE' + NCHAR(13);

			IF @Debug = 1 OR @Execute = 'N'
			BEGIN
				IF @sql IS NULL PRINT '@sql is NULL for SET RECOVERY SIMPLE: @RestoreDatabaseName';
				PRINT @sql;
			END; 

		IF @Debug IN (0, 1) AND @Execute = 'Y'
			EXECUTE @sql = [dbo].[CommandExecute] @DatabaseContext=N'master', @Command = @sql, @CommandType = 'SIMPLE LOGGING', @Mode = 1, @DatabaseName = @UnquotedRestoreDatabaseName, @LogToTable = 'Y', @Execute = 'Y';
	END;	    

 -- Run checkdb against this database
IF @RunCheckDB = 1
	BEGIN
		SET @sql = N'DBCC CHECKDB (' + @RestoreDatabaseName + N') WITH NO_INFOMSGS, ALL_ERRORMSGS, DATA_PURITY;';
			
			IF @Debug = 1 OR @Execute = 'N'
			BEGIN
				IF @sql IS NULL PRINT '@sql is NULL for Run Integrity Check: @RestoreDatabaseName';
				PRINT @sql;
			END; 
		
		IF @Debug IN (0, 1) AND @Execute = 'Y'
			EXECUTE @sql = [dbo].[CommandExecute] @DatabaseContext=N'master', @Command = @sql, @CommandType = 'DBCC CHECKDB', @Mode = 1, @DatabaseName = @UnquotedRestoreDatabaseName, @LogToTable = 'Y', @Execute = 'Y';
	END;


IF @DatabaseOwner IS NOT NULL
	BEGIN
		IF @RunRecovery = 1
		BEGIN
			IF EXISTS (SELECT * FROM master.dbo.syslogins WHERE syslogins.loginname = @DatabaseOwner)
			BEGIN
				SET @sql = N'ALTER AUTHORIZATION ON DATABASE::' + @RestoreDatabaseName + ' TO [' + @DatabaseOwner + ']';

					IF @Debug = 1 OR @Execute = 'N'
					BEGIN
						IF @sql IS NULL PRINT '@sql is NULL for Set Database Owner';
						PRINT @sql;
					END;

				IF @Debug IN (0, 1) AND @Execute = 'Y'
					EXECUTE @sql = [dbo].[CommandExecute] @DatabaseContext=N'master',@Command = @sql, @CommandType = 'ALTER AUTHORIZATION', @Mode = 1, @DatabaseName = @UnquotedRestoreDatabaseName, @LogToTable = 'Y', @Execute = 'Y';
			END
			ELSE
			BEGIN
				PRINT @DatabaseOwner + ' is not a valid Login. Database Owner not set.';
			END
		END
		ELSE
		BEGIN
			PRINT @RestoreDatabaseName + ' is still in Recovery, so we are unable to change the database owner to [' + @DatabaseOwner + '].';
		END
	END;

	IF @SetTrustworthyON = 1
		BEGIN
			IF @RunRecovery = 1
			BEGIN
				IF IS_SRVROLEMEMBER('sysadmin') = 1
				BEGIN
					SET @sql = N'ALTER DATABASE ' + @RestoreDatabaseName + N' SET TRUSTWORTHY ON;';

						IF @Debug = 1 OR @Execute = 'N'
						BEGIN
							IF @sql IS NULL PRINT '@sql is NULL for SET TRUSTWORTHY ON';
							PRINT @sql;
						END;

					IF @Debug IN (0, 1) AND @Execute = 'Y'
						EXECUTE @sql = [dbo].[CommandExecute] @DatabaseContext=N'master', @Command = @sql, @CommandType = 'ALTER DATABASE', @Mode = 1, @DatabaseName = @UnquotedRestoreDatabaseName, @LogToTable = 'Y', @Execute = 'Y';
				END
				ELSE
				BEGIN
					PRINT 'Current user''s login is NOT a member of the sysadmin role. Database TRUSTWORHY bit has not been enabled.';
				END
			END
			ELSE
			BEGIN
				PRINT @RestoreDatabaseName + ' is still in Recovery, so we are unable to enable the TRUSTWORHY bit.';
			END
		END;

-- Link a user entry in the sys.database_principals system catalog view in the restored database to a SQL Server login of the same name
IF @FixOrphanUsers = 1 
	BEGIN
		SET @sql = N'
-- Fixup Orphan Users by setting database user sid to match login sid
DECLARE @FixOrphansSql NVARCHAR(MAX);
DECLARE @OrphanUsers TABLE (SqlToExecute NVARCHAR(MAX));
USE ' + @RestoreDatabaseName + ';

INSERT @OrphanUsers
SELECT ''ALTER USER ['' + d.name + ''] WITH LOGIN = ['' + d.name + '']; ''
	FROM  sys.database_principals d
	INNER JOIN master.sys.server_principals s ON d.name COLLATE DATABASE_DEFAULT = s.name COLLATE DATABASE_DEFAULT
	WHERE d.type_desc = ''SQL_USER''
		AND d.name NOT IN (''guest'',''dbo'')
		AND d.sid <> s.sid
	ORDER BY d.name;

SELECT @FixOrphansSql = (SELECT SqlToExecute AS [text()] FROM @OrphanUsers FOR XML PATH (''''), TYPE).value(''text()[1]'',''NVARCHAR(MAX)'');

IF @FixOrphansSql IS NULL 
	PRINT ''No orphan users require a sid fixup.'';
ELSE
BEGIN
	PRINT ''Fix Orphan Users: '' + @FixOrphansSql;
	EXECUTE(@FixOrphansSql);
END;'

		IF @Debug = 1 OR @Execute = 'N'
		BEGIN
			IF @sql IS NULL PRINT '@sql is NULL for Fix Orphan Users';
			PRINT @sql;
		END;

		IF @Debug IN (0, 1) AND @Execute = 'Y'
			EXECUTE [dbo].[CommandExecute] @DatabaseContext = 'master', @Command = @sql, @CommandType = 'UPDATE', @Mode = 1, @DatabaseName = @UnquotedRestoreDatabaseName, @LogToTable = 'Y', @Execute = 'Y';
	END; 

IF @RunStoredProcAfterRestore IS NOT NULL AND LEN(LTRIM(@RunStoredProcAfterRestore)) > 0
BEGIN
	PRINT 'Attempting to run ' + @RunStoredProcAfterRestore
	SET @sql = N'EXEC ' + @RestoreDatabaseName + '.' + @RunStoredProcAfterRestore

	IF @Debug = 1 OR @Execute = 'N'
	BEGIN
		IF @sql IS NULL PRINT '@sql is NULL when building for @RunStoredProcAfterRestore'
		PRINT @sql
	END
			
	IF @RunRecovery = 0
	BEGIN
		PRINT 'Unable to run Run Stored Procedure After Restore as database is not recovered. Run command again with @RunRecovery = 1'
	END
	ELSE
    BEGIN
		IF @Debug IN (0, 1) AND @Execute = 'Y'
			EXEC sp_executesql  @sql
	END
END
 -- If test restore then blow the database away (be careful)
IF @TestRestore = 1
	BEGIN
		SET @sql = N'DROP DATABASE ' + @RestoreDatabaseName + NCHAR(13);
			
			IF @Debug = 1 OR @Execute = 'N'
			BEGIN
				IF @sql IS NULL PRINT '@sql is NULL for DROP DATABASE: @RestoreDatabaseName';
				PRINT @sql;
			END; 
		
		IF @Debug IN (0, 1) AND @Execute = 'Y'
			EXECUTE @sql = [dbo].[CommandExecute] @DatabaseContext=N'master',@Command = @sql, @CommandType = 'DROP DATABASE', @Mode = 1, @DatabaseName = @UnquotedRestoreDatabaseName, @LogToTable = 'Y', @Execute = 'Y';

	END;

-- Clean-Up Tempdb Objects
IF OBJECT_ID( 'tempdb..#SplitFullBackups' ) IS NOT NULL DROP TABLE #SplitFullBackups;
IF OBJECT_ID( 'tempdb..#SplitDiffBackups' ) IS NOT NULL DROP TABLE #SplitDiffBackups;
IF OBJECT_ID( 'tempdb..#SplitLogBackups' ) IS NOT NULL DROP TABLE #SplitLogBackups;


-- END of sp_DatabaseRestore
GO

-- START of sp_whoisactive

Use [master];
GO

SET QUOTED_IDENTIFIER ON;
SET ANSI_PADDING ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET ANSI_WARNINGS ON;
SET NUMERIC_ROUNDABORT OFF;
SET ARITHABORT ON;
GO

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = 'sp_WhoIsActive')
    EXEC ('CREATE PROC dbo.sp_WhoIsActive AS SELECT ''stub version, to be replaced''')
GO

/*********************************************************************************************
Who Is Active? v12.00 (2021-11-10)
(C) 2007-2021, Adam Machanic

Feedback: https://github.com/amachanic/sp_whoisactive/issues
Releases: https://github.com/amachanic/sp_whoisactive/releases
Docs: http://whoisactive.com

License:
    https://github.com/amachanic/sp_whoisactive/blob/master/LICENSE
*********************************************************************************************/
ALTER PROC dbo.sp_WhoIsActive
(
--~
    --Filters--Both inclusive and exclusive
    --Set either filter to '' to disable
    --Valid filter types are: session, program, database, login, and host
    --Session is a session ID, and either 0 or '' can be used to indicate "all" sessions
    --All other filter types support % or _ as wildcards
    @filter sysname = '',
    @filter_type VARCHAR(10) = 'session',
    @not_filter sysname = '',
    @not_filter_type VARCHAR(10) = 'session',

    --Retrieve data about the calling session?
    @show_own_spid BIT = 0,

    --Retrieve data about system sessions?
    @show_system_spids BIT = 0,

    --Controls how sleeping SPIDs are handled, based on the idea of levels of interest
    --0 does not pull any sleeping SPIDs
    --1 pulls only those sleeping SPIDs that also have an open transaction
    --2 pulls all sleeping SPIDs
    @show_sleeping_spids TINYINT = 1,

    --If 1, gets the full stored procedure or running batch, when available
    --If 0, gets only the actual statement that is currently running in the batch or procedure
    @get_full_inner_text BIT = 0,

    --Get associated query plans for running tasks, if available
    --If @get_plans = 1, gets the plan based on the request's statement offset
    --If @get_plans = 2, gets the entire plan based on the request's plan_handle
    @get_plans TINYINT = 0,

    --Get the associated outer ad hoc query or stored procedure call, if available
    @get_outer_command BIT = 0,

    --Enables pulling transaction log write info, transaction duration, and the
    --implicit_transaction identification column
    @get_transaction_info BIT = 0,

    --Get information on active tasks, based on three interest levels
    --Level 0 does not pull any task-related information
    --Level 1 is a lightweight mode that pulls the top non-CXPACKET wait, giving preference to blockers
    --Level 2 pulls all available task-based metrics, including:
    --number of active tasks, current wait stats, physical I/O, context switches, and blocker information
    @get_task_info TINYINT = 1,

    --Gets associated locks for each request, aggregated in an XML format
    @get_locks BIT = 0,

    --Get average time for past runs of an active query
    --(based on the combination of plan handle, sql handle, and offset)
    @get_avg_time BIT = 0,

    --Get additional non-performance-related information about the session or request
    --text_size, language, date_format, date_first, quoted_identifier, arithabort, ansi_null_dflt_on,
    --ansi_defaults, ansi_warnings, ansi_padding, ansi_nulls, concat_null_yields_null,
    --transaction_isolation_level, lock_timeout, deadlock_priority, row_count, command_type
    --
    --If a SQL Agent job is running, an subnode called agent_info will be populated with some or all of
    --the following: job_id, job_name, step_id, step_name, msdb_query_error (in the event of an error)
    --
    --If @get_task_info is set to 2 and a lock wait is detected, a subnode called block_info will be
    --populated with some or all of the following: lock_type, database_name, object_id, file_id, hobt_id,
    --applock_hash, metadata_resource, metadata_class_id, object_name, schema_name
    @get_additional_info BIT = 0,

    --Get additional information related to workspace memory
    --requested_memory, granted_memory, max_used_memory, and memory_info.
    --
    --Not available for SQL Server 2005.
    @get_memory_info BIT = 0,

    --Walk the blocking chain and count the number of
    --total SPIDs blocked all the way down by a given session
    --Also enables task_info Level 1, if @get_task_info is set to 0
    @find_block_leaders BIT = 0,

    --Pull deltas on various metrics
    --Interval in seconds to wait before doing the second data pull
    @delta_interval TINYINT = 0,

    --List of desired output columns, in desired order
    --Note that the final output will be the intersection of all enabled features and all
    --columns in the list. Therefore, only columns associated with enabled features will
    --actually appear in the output. Likewise, removing columns from this list may effectively
    --disable features, even if they are turned on
    --
    --Each element in this list must be one of the valid output column names. Names must be
    --delimited by square brackets. White space, formatting, and additional characters are
    --allowed, as long as the list contains exact matches of delimited valid column names.
    --@output_column_list VARCHAR(8000) = '[dd%][session_id][sql_text][sql_command][login_name][wait_info][tasks][tran_log%][cpu%][temp%][block%][reads%][writes%][context%][physical%][query_plan][locks][%]',
	@output_column_list VARCHAR(8000) = '[session_id][sql_text][sql_command][dd%][login_name][database_name][percent_complete][wait_info][block%][status][host_name][program_name][tasks][tran_log%][cpu%][temp%][reads%][writes%][context%][physical%][query_plan][locks][%]',

    --Column(s) by which to sort output, optionally with sort directions.
        --Valid column choices:
        --session_id, physical_io, reads, physical_reads, writes, tempdb_allocations,
        --tempdb_current, CPU, context_switches, used_memory, physical_io_delta, reads_delta,
        --physical_reads_delta, writes_delta, tempdb_allocations_delta, tempdb_current_delta,
        --CPU_delta, context_switches_delta, used_memory_delta, tasks, tran_start_time,
        --open_tran_count, blocking_session_id, blocked_session_count, percent_complete,
        --host_name, login_name, database_name, start_time, login_time, program_name
        --
        --Note that column names in the list must be bracket-delimited. Commas and/or white
        --space are not required.
    @sort_order VARCHAR(500) = '[start_time] ASC',

    --Formats some of the output columns in a more "human readable" form
    --0 disables outfput format
    --1 formats the output for variable-width fonts
    --2 formats the output for fixed-width fonts
    @format_output TINYINT = 1,

    --If set to a non-blank value, the script will attempt to insert into the specified
    --destination table. Please note that the script will not verify that the table exists,
    --or that it has the correct schema, before doing the insert.
    --Table can be specified in one, two, or three-part format
    @destination_table VARCHAR(4000) = '',

    --If set to 1, no data collection will happen and no result set will be returned; instead,
    --a CREATE TABLE statement will be returned via the @schema parameter, which will match
    --the schema of the result set that would be returned by using the same collection of the
    --rest of the parameters. The CREATE TABLE statement will have a placeholder token of
    --<table_name> in place of an actual table name.
    @return_schema BIT = 0,
    @schema VARCHAR(MAX) = NULL OUTPUT,

    --Help! What do I do?
    @help BIT = 0
--~
)
/*
OUTPUT COLUMNS
--------------
Formatted/Non:    [session_id] [smallint] NOT NULL
    Session ID (a.k.a. SPID)

Formatted:        [dd hh:mm:ss.mss] [varchar](15) NULL
Non-Formatted:    <not returned>
    For an active request, time the query has been running
    For a sleeping session, time since the last batch completed

Formatted:        [dd hh:mm:ss.mss (avg)] [varchar](15) NULL
Non-Formatted:    [avg_elapsed_time] [int] NULL
    (Requires @get_avg_time option)
    How much time has the active portion of the query taken in the past, on average?

Formatted:        [physical_io] [varchar](30) NULL
Non-Formatted:    [physical_io] [bigint] NULL
    Shows the number of physical I/Os, for active requests

Formatted:        [reads] [varchar](30) NULL
Non-Formatted:    [reads] [bigint] NULL
    For an active request, number of reads done for the current query
    For a sleeping session, total number of reads done over the lifetime of the session

Formatted:        [physical_reads] [varchar](30) NULL
Non-Formatted:    [physical_reads] [bigint] NULL
    For an active request, number of physical reads done for the current query
    For a sleeping session, total number of physical reads done over the lifetime of the session

Formatted:        [writes] [varchar](30) NULL
Non-Formatted:    [writes] [bigint] NULL
    For an active request, number of writes done for the current query
    For a sleeping session, total number of writes done over the lifetime of the session

Formatted:        [tempdb_allocations] [varchar](30) NULL
Non-Formatted:    [tempdb_allocations] [bigint] NULL
    For an active request, number of TempDB writes done for the current query
    For a sleeping session, total number of TempDB writes done over the lifetime of the session

Formatted:        [tempdb_current] [varchar](30) NULL
Non-Formatted:    [tempdb_current] [bigint] NULL
    For an active request, number of TempDB pages currently allocated for the query
    For a sleeping session, number of TempDB pages currently allocated for the session

Formatted:        [CPU] [varchar](30) NULL
Non-Formatted:    [CPU] [bigint] NULL
    For an active request, total CPU time consumed by the current query
    For a sleeping session, total CPU time consumed over the lifetime of the session

Formatted:        [context_switches] [varchar](30) NULL
Non-Formatted:    [context_switches] [bigint] NULL
    Shows the number of context switches, for active requests

Formatted:        [used_memory] [varchar](30) NOT NULL
Non-Formatted:    [used_memory] [bigint] NOT NULL
    For an active request, total memory consumption for the current query
    For a sleeping session, total current memory consumption

Formatted:        [max_used_memory] [varchar](30) NULL
Non-Formatted:    [max_used_memory] [bigint] NULL
    (Requires @get_memory_info = 1)
    For an active request, the maximum amount of memory that has been used during
    processing up to the point of observation for the current query

Formatted:        [requested_memory] [varchar](30) NULL
Non-Formatted:    [requested_memory] [bigint] NULL
    (Requires @get_memory_info = 1)
    For an active request, the amount of memory requested by the query processor
    for hash, sort, and parallelism operations

Formatted:        [granted_memory] [varchar](30) NULL
Non-Formatted:    [granted_memory] [bigint] NULL
    (Requires @get_memory_info = 1)
    For an active request, the amount of memory granted to the query processor
    for hash, sort, and parallelism operations

Formatted:        [physical_io_delta] [varchar](30) NULL
Non-Formatted:    [physical_io_delta] [bigint] NULL
    (Requires @delta_interval option)
    Difference between the number of physical I/Os reported on the first and second collections.
    If the request started after the first collection, the value will be NULL

Formatted:        [reads_delta] [varchar](30) NULL
Non-Formatted:    [reads_delta] [bigint] NULL
    (Requires @delta_interval option)
    Difference between the number of reads reported on the first and second collections.
    If the request started after the first collection, the value will be NULL

Formatted:        [physical_reads_delta] [varchar](30) NULL
Non-Formatted:    [physical_reads_delta] [bigint] NULL
    (Requires @delta_interval option)
    Difference between the number of physical reads reported on the first and second collections.
    If the request started after the first collection, the value will be NULL

Formatted:        [writes_delta] [varchar](30) NULL
Non-Formatted:    [writes_delta] [bigint] NULL
    (Requires @delta_interval option)
    Difference between the number of writes reported on the first and second collections.
    If the request started after the first collection, the value will be NULL

Formatted:        [tempdb_allocations_delta] [varchar](30) NULL
Non-Formatted:    [tempdb_allocations_delta] [bigint] NULL
    (Requires @delta_interval option)
    Difference between the number of TempDB writes reported on the first and second collections.
    If the request started after the first collection, the value will be NULL

Formatted:        [tempdb_current_delta] [varchar](30) NULL
Non-Formatted:    [tempdb_current_delta] [bigint] NULL
    (Requires @delta_interval option)
    Difference between the number of allocated TempDB pages reported on the first and second
    collections. If the request started after the first collection, the value will be NULL

Formatted:        [CPU_delta] [varchar](30) NULL
Non-Formatted:    [CPU_delta] [int] NULL
    (Requires @delta_interval option)
    Difference between the CPU time reported on the first and second collections.
    If the request started after the first collection, the value will be NULL

Formatted:        [context_switches_delta] [varchar](30) NULL
Non-Formatted:    [context_switches_delta] [bigint] NULL
    (Requires @delta_interval option)
    Difference between the context switches count reported on the first and second collections
    If the request started after the first collection, the value will be NULL

Formatted:        [used_memory_delta] [varchar](30) NULL
Non-Formatted:    [used_memory_delta] [bigint] NULL
    Difference between the memory usage reported on the first and second collections
    If the request started after the first collection, the value will be NULL

Formatted:        [max_used_memory_delta] [varchar](30) NULL
Non-Formatted:    [max_used_memory_delta] [bigint] NULL
    Difference between the max memory usage reported on the first and second collections
    If the request started after the first collection, the value will be NULL

Formatted:        [tasks] [varchar](30) NULL
Non-Formatted:    [tasks] [smallint] NULL
    Number of worker tasks currently allocated, for active requests

Formatted/Non:    [status] [varchar](30) NOT NULL
    Activity status for the session (running, sleeping, etc)

Formatted/Non:    [wait_info] [nvarchar](4000) NULL
    Aggregates wait information, in the following format:
        (Ax: Bms/Cms/Dms)E
    A is the number of waiting tasks currently waiting on resource type E. B/C/D are wait
    times, in milliseconds. If only one thread is waiting, its wait time will be shown as B.
    If two tasks are waiting, each of their wait times will be shown (B/C). If three or more
    tasks are waiting, the minimum, average, and maximum wait times will be shown (B/C/D).
    If wait type E is a page latch wait and the page is of a "special" type (e.g. PFS, GAM, SGAM),
    the page type will be identified.
    If wait type E is CXPACKET, CXCONSUMER, CXSYNC_PORT, or CXSYNC_CONSUMER the nodeId from the
    query plan will be identified

Formatted/Non:    [locks] [xml] NULL
    (Requires @get_locks option)
    Aggregates lock information, in XML format.
    The lock XML includes the lock mode, locked object, and aggregates the number of requests.
    Attempts are made to identify locked objects by name

Formatted/Non:    [tran_start_time] [datetime] NULL
    (Requires @get_transaction_info option)
    Date and time that the first transaction opened by a session caused a transaction log
    write to occur.

Formatted/Non:    [tran_log_writes] [nvarchar](4000) NULL
    (Requires @get_transaction_info option)
    Aggregates transaction log write information, in the following format:
    A:wB (C kB)
    A is a database that has been touched by an active transaction
    B is the number of log writes that have been made in the database as a result of the transaction
    C is the number of log kilobytes consumed by the log records

Formatted/Non:    [implicit_tran] [nvarchar](3) NULL
    (Requires @get_transaction_info option)
    For active read-write transactions, returns on "ON" the transaction has been started as a result
    of the session using the implicit_transactions option, or "OFF" otherwise.

Formatted:        [open_tran_count] [varchar](30) NULL
Non-Formatted:    [open_tran_count] [smallint] NULL
    Shows the number of open transactions the session has open

Formatted:        [sql_command] [xml] NULL
Non-Formatted:    [sql_command] [nvarchar](max) NULL
    (Requires @get_outer_command option)
    Shows the "outer" SQL command, i.e. the text of the batch or RPC sent to the server,
    if available

Formatted:        [sql_text] [xml] NULL
Non-Formatted:    [sql_text] [nvarchar](max) NULL
    Shows the SQL text for active requests or the last statement executed
    for sleeping sessions, if available in either case.
    If @get_full_inner_text option is set, shows the full text of the batch.
    Otherwise, shows only the active statement within the batch.
    If the query text is locked, a special timeout message will be sent, in the following format:
        <timeout_exceeded />
    If an error occurs, an error message will be sent, in the following format:
        <error message="message" />

Formatted/Non:    [query_plan] [xml] NULL
    (Requires @get_plans option)
    Shows the query plan for the request, if available.
    If the plan is locked, a special timeout message will be sent, in the following format:
        <timeout_exceeded />
    If an error occurs, an error message will be sent, in the following format:
        <error message="message" />

Formatted/Non:    [blocking_session_id] [smallint] NULL
    When applicable, shows the blocking SPID

Formatted:        [blocked_session_count] [varchar](30) NULL
Non-Formatted:    [blocked_session_count] [smallint] NULL
    (Requires @find_block_leaders option)
    The total number of SPIDs blocked by this session,
    all the way down the blocking chain.

Formatted:        [percent_complete] [varchar](30) NULL
Non-Formatted:    [percent_complete] [real] NULL
    When applicable, shows the percent complete (e.g. for backups, restores, and some rollbacks)

Formatted/Non:    [host_name] [sysname] NOT NULL
    Shows the host name for the connection

Formatted/Non:    [login_name] [sysname] NOT NULL
    Shows the login name for the connection

Formatted/Non:    [database_name] [sysname] NULL
    Shows the connected database

Formatted/Non:    [program_name] [sysname] NULL
    Shows the reported program/application name

Formatted/Non:    [additional_info] [xml] NULL
    (Requires @get_additional_info option)
    Returns additional non-performance-related session/request information
    If the script finds a SQL Agent job running, the name of the job and job step will be reported
    If @get_task_info = 2 and the script finds a lock wait, the locked object will be reported

Formatted/Non:    [start_time] [datetime] NOT NULL
    For active requests, shows the time the request started
    For sleeping sessions, shows the time the last batch completed

Formatted/Non:    [login_time] [datetime] NOT NULL
    Shows the time that the session connected

Formatted/Non:    [request_id] [int] NULL
    For active requests, shows the request_id
    Should be 0 unless MARS is being used

Formatted/Non:    [collection_time] [datetime] NOT NULL
    Time that this script's final SELECT ran

Formatted/Non:    [memory_info] [xml] NULL
    (Requires @get_memory_info)
    For active queries that require workspace memory, returns information on memory grants,
    resource semaphores, and the resource governor settings that are impacting the allocation.
*/
AS
BEGIN;
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
    SET QUOTED_IDENTIFIER ON;
    SET ANSI_PADDING ON;
    SET CONCAT_NULL_YIELDS_NULL ON;
    SET ANSI_WARNINGS ON;
    SET NUMERIC_ROUNDABORT OFF;
    SET ARITHABORT ON;

    IF
        @filter IS NULL
        OR @filter_type IS NULL
        OR @not_filter IS NULL
        OR @not_filter_type IS NULL
        OR @show_own_spid IS NULL
        OR @show_system_spids IS NULL
        OR @show_sleeping_spids IS NULL
        OR @get_full_inner_text IS NULL
        OR @get_plans IS NULL
        OR @get_outer_command IS NULL
        OR @get_transaction_info IS NULL
        OR @get_task_info IS NULL
        OR @get_locks IS NULL
        OR @get_avg_time IS NULL
        OR @get_additional_info IS NULL
        OR @find_block_leaders IS NULL
        OR @delta_interval IS NULL
        OR @format_output IS NULL
        OR @output_column_list IS NULL
        OR @sort_order IS NULL
        OR @return_schema IS NULL
        OR @destination_table IS NULL
        OR @help IS NULL
    BEGIN;
        RAISERROR('Input parameters cannot be NULL', 16, 1);
        RETURN;
    END;
   
    IF @filter_type NOT IN ('session', 'program', 'database', 'login', 'host')
    BEGIN;
        RAISERROR('Valid filter types are: session, program, database, login, host', 16, 1);
        RETURN;
    END;
   
    IF @filter_type = 'session' AND @filter LIKE '%[^0123456789]%'
    BEGIN;
        RAISERROR('Session filters must be valid integers', 16, 1);
        RETURN;
    END;
   
    IF @not_filter_type NOT IN ('session', 'program', 'database', 'login', 'host')
    BEGIN;
        RAISERROR('Valid filter types are: session, program, database, login, host', 16, 1);
        RETURN;
    END;
   
    IF @not_filter_type = 'session' AND @not_filter LIKE '%[^0123456789]%'
    BEGIN;
        RAISERROR('Session filters must be valid integers', 16, 1);
        RETURN;
    END;
   
    IF @show_sleeping_spids NOT IN (0, 1, 2)
    BEGIN;
        RAISERROR('Valid values for @show_sleeping_spids are: 0, 1, or 2', 16, 1);
        RETURN;
    END;
   
    IF @get_plans NOT IN (0, 1, 2)
    BEGIN;
        RAISERROR('Valid values for @get_plans are: 0, 1, or 2', 16, 1);
        RETURN;
    END;

    IF @get_task_info NOT IN (0, 1, 2)
    BEGIN;
        RAISERROR('Valid values for @get_task_info are: 0, 1, or 2', 16, 1);
        RETURN;
    END;

    IF @format_output NOT IN (0, 1, 2)
    BEGIN;
        RAISERROR('Valid values for @format_output are: 0, 1, or 2', 16, 1);
        RETURN;
    END;

    IF @get_memory_info = 1 AND NOT EXISTS (SELECT * FROM sys.all_objects WHERE name = 'resource_governor_resource_pools')
    BEGIN;
        RAISERROR('@get_memory_info is not available for SQL Server 2005.', 16, 1);
        RETURN;
    END;

    IF @help = 1
    BEGIN;
        DECLARE
            @header VARCHAR(MAX),
            @params VARCHAR(MAX),
            @outputs VARCHAR(MAX);

        SELECT
            @header =
                REPLACE
                (
                    REPLACE
                    (
                        CONVERT
                        (
                            VARCHAR(MAX),
                            SUBSTRING
                            (
                                t.text,
                                CHARINDEX('/' + REPLICATE('*', 93), t.text) + 94,
                                CHARINDEX(REPLICATE('*', 93) + '/', t.text) - (CHARINDEX('/' + REPLICATE('*', 93), t.text) + 94)
                            )
                        ),
                        CHAR(13)+CHAR(10),
                        CHAR(13)
                    ),
                    '    ',
                    ''
                ),
            @params =
                CHAR(13) +
                    REPLACE
                    (
                        REPLACE
                        (
                            CONVERT
                            (
                                VARCHAR(MAX),
                                SUBSTRING
                                (
                                    t.text,
                                    CHARINDEX('--~', t.text) + 5,
                                    CHARINDEX('--~', t.text, CHARINDEX('--~', t.text) + 5) - (CHARINDEX('--~', t.text) + 5)
                                )
                            ),
                            CHAR(13)+CHAR(10),
                            CHAR(13)
                        ),
                        '    ',
                        ''
                    ),
                @outputs =
                    CHAR(13) +
                        REPLACE
                        (
                            REPLACE
                            (
                                REPLACE
                                (
                                    CONVERT
                                    (
                                        VARCHAR(MAX),
                                        SUBSTRING
                                        (
                                            t.text,
                                            CHARINDEX('OUTPUT COLUMNS'+CHAR(13)+CHAR(10)+'--------------', t.text) + 32,
                                            CHARINDEX('*/', t.text, CHARINDEX('OUTPUT COLUMNS'+CHAR(13)+CHAR(10)+'--------------', t.text) + 32) - (CHARINDEX('OUTPUT COLUMNS'+CHAR(13)+CHAR(10)+'--------------', t.text) + 32)
                                        )
                                    ),
                                    '    ',
                                    CHAR(255)
                                ),
                                CHAR(13)+CHAR(10),
                                CHAR(13)
                            ),
                            '    ',
                            ''
                        ) +
                        CHAR(13)
        FROM sys.dm_exec_requests AS r
        CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) AS t
        WHERE
            r.session_id = @@SPID;

        WITH
        a0 AS
        (SELECT 1 AS n UNION ALL SELECT 1),
        a1 AS
        (SELECT 1 AS n FROM a0 AS a CROSS JOIN a0 AS b),
        a2 AS
        (SELECT 1 AS n FROM a1 AS a CROSS JOIN a1 AS b),
        a3 AS
        (SELECT 1 AS n FROM a2 AS a CROSS JOIN a2 AS b),
        a4 AS
        (SELECT 1 AS n FROM a3 AS a CROSS JOIN a3 AS b),
        numbers AS
        (
            SELECT TOP(LEN(@header) - 1)
                ROW_NUMBER() OVER
                (
                    ORDER BY (SELECT NULL)
                ) AS number
            FROM a4
            ORDER BY
                number
        )
        SELECT
            RTRIM(LTRIM(
                SUBSTRING
                (
                    @header,
                    number + 1,
                    CHARINDEX(CHAR(13), @header, number + 1) - number - 1
                )
            )) AS [------header---------------------------------------------------------------------------------------------------------------]
        FROM numbers
        WHERE
            SUBSTRING(@header, number, 1) = CHAR(13);

        WITH
        a0 AS
        (SELECT 1 AS n UNION ALL SELECT 1),
        a1 AS
        (SELECT 1 AS n FROM a0 AS a CROSS JOIN a0 AS b),
        a2 AS
        (SELECT 1 AS n FROM a1 AS a CROSS JOIN a1 AS b),
        a3 AS
        (SELECT 1 AS n FROM a2 AS a CROSS JOIN a2 AS b),
        a4 AS
        (SELECT 1 AS n FROM a3 AS a CROSS JOIN a3 AS b),
        numbers AS
        (
            SELECT TOP(LEN(@params) - 1)
                ROW_NUMBER() OVER
                (
                    ORDER BY (SELECT NULL)
                ) AS number
            FROM a4
            ORDER BY
                number
        ),
        tokens AS
        (
            SELECT
                RTRIM(LTRIM(
                    SUBSTRING
                    (
                        @params,
                        number + 1,
                        CHARINDEX(CHAR(13), @params, number + 1) - number - 1
                    )
                )) AS token,
                number,
                CASE
                    WHEN SUBSTRING(@params, number + 1, 1) = CHAR(13) THEN number
                    ELSE COALESCE(NULLIF(CHARINDEX(',' + CHAR(13) + CHAR(13), @params, number), 0), LEN(@params))
                END AS param_group,
                ROW_NUMBER() OVER
                (
                    PARTITION BY
                        CHARINDEX(',' + CHAR(13) + CHAR(13), @params, number),
                        SUBSTRING(@params, number+1, 1)
                    ORDER BY
                        number
                ) AS group_order
            FROM numbers
            WHERE
                SUBSTRING(@params, number, 1) = CHAR(13)
        ),
        parsed_tokens AS
        (
            SELECT
                MIN
                (
                    CASE
                        WHEN token LIKE '@%' THEN token
                        ELSE NULL
                    END
                ) AS parameter,
                MIN
                (
                    CASE
                        WHEN token LIKE '--%' THEN RIGHT(token, LEN(token) - 2)
                        ELSE NULL
                    END
                ) AS description,
                param_group,
                group_order
            FROM tokens
            WHERE
                NOT
                (
                    token = ''
                    AND group_order > 1
                )
            GROUP BY
                param_group,
                group_order
        )
        SELECT
            CASE
                WHEN description IS NULL AND parameter IS NULL THEN '-------------------------------------------------------------------------'
                WHEN param_group = MAX(param_group) OVER() THEN parameter
                ELSE COALESCE(LEFT(parameter, LEN(parameter) - 1), '')
            END AS [------parameter----------------------------------------------------------],
            CASE
                WHEN description IS NULL AND parameter IS NULL THEN '----------------------------------------------------------------------------------------------------------------------'
                ELSE COALESCE(description, '')
            END AS [------description-----------------------------------------------------------------------------------------------------]
        FROM parsed_tokens
        ORDER BY
            param_group,
            group_order;
       
        WITH
        a0 AS
        (SELECT 1 AS n UNION ALL SELECT 1),
        a1 AS
        (SELECT 1 AS n FROM a0 AS a CROSS JOIN a0 AS b),
        a2 AS
        (SELECT 1 AS n FROM a1 AS a CROSS JOIN a1 AS b),
        a3 AS
        (SELECT 1 AS n FROM a2 AS a CROSS JOIN a2 AS b),
        a4 AS
        (SELECT 1 AS n FROM a3 AS a CROSS JOIN a3 AS b),
        numbers AS
        (
            SELECT TOP(LEN(@outputs) - 1)
                ROW_NUMBER() OVER
                (
                    ORDER BY (SELECT NULL)
                ) AS number
            FROM a4
            ORDER BY
                number
        ),
        tokens AS
        (
            SELECT
                RTRIM(LTRIM(
                    SUBSTRING
                    (
                        @outputs,
                        number + 1,
                        CASE
                            WHEN
                                COALESCE(NULLIF(CHARINDEX(CHAR(13) + 'Formatted', @outputs, number + 1), 0), LEN(@outputs)) <
                                    COALESCE(NULLIF(CHARINDEX(CHAR(13) + CHAR(255) COLLATE Latin1_General_Bin2, @outputs, number + 1), 0), LEN(@outputs))
                                THEN COALESCE(NULLIF(CHARINDEX(CHAR(13) + 'Formatted', @outputs, number + 1), 0), LEN(@outputs)) - number - 1
                            ELSE
                                COALESCE(NULLIF(CHARINDEX(CHAR(13) + CHAR(255) COLLATE Latin1_General_Bin2, @outputs, number + 1), 0), LEN(@outputs)) - number - 1
                        END
                    )
                )) AS token,
                number,
                COALESCE(NULLIF(CHARINDEX(CHAR(13) + 'Formatted', @outputs, number + 1), 0), LEN(@outputs)) AS output_group,
                ROW_NUMBER() OVER
                (
                    PARTITION BY
                        COALESCE(NULLIF(CHARINDEX(CHAR(13) + 'Formatted', @outputs, number + 1), 0), LEN(@outputs))
                    ORDER BY
                        number
                ) AS output_group_order
            FROM numbers
            WHERE
                SUBSTRING(@outputs, number, 10) = CHAR(13) + 'Formatted'
                OR SUBSTRING(@outputs, number, 2) = CHAR(13) + CHAR(255) COLLATE Latin1_General_Bin2
        ),
        output_tokens AS
        (
            SELECT
                *,
                CASE output_group_order
                    WHEN 2 THEN MAX(CASE output_group_order WHEN 1 THEN token ELSE NULL END) OVER (PARTITION BY output_group)
                    ELSE ''
                END COLLATE Latin1_General_Bin2 AS column_info
            FROM tokens
        )
        SELECT
            CASE output_group_order
                WHEN 1 THEN '-----------------------------------'
                WHEN 2 THEN
                    CASE
                        WHEN CHARINDEX('Formatted/Non:', column_info) = 1 THEN
                            SUBSTRING(column_info, CHARINDEX(CHAR(255) COLLATE Latin1_General_Bin2, column_info)+1, CHARINDEX(']', column_info, CHARINDEX(CHAR(255) COLLATE Latin1_General_Bin2, column_info)+2) - CHARINDEX(CHAR(255) COLLATE Latin1_General_Bin2, column_info))
                        ELSE
                            SUBSTRING(column_info, CHARINDEX(CHAR(255) COLLATE Latin1_General_Bin2, column_info)+2, CHARINDEX(']', column_info, CHARINDEX(CHAR(255) COLLATE Latin1_General_Bin2, column_info)+2) - CHARINDEX(CHAR(255) COLLATE Latin1_General_Bin2, column_info)-1)
                    END
                ELSE ''
            END AS formatted_column_name,
            CASE output_group_order
                WHEN 1 THEN '-----------------------------------'
                WHEN 2 THEN
                    CASE
                        WHEN CHARINDEX('Formatted/Non:', column_info) = 1 THEN
                            SUBSTRING(column_info, CHARINDEX(']', column_info)+2, LEN(column_info))
                        ELSE
                            SUBSTRING(column_info, CHARINDEX(']', column_info)+2, CHARINDEX('Non-Formatted:', column_info, CHARINDEX(']', column_info)+2) - CHARINDEX(']', column_info)-3)
                    END
                ELSE ''
            END AS formatted_column_type,
            CASE output_group_order
                WHEN 1 THEN '---------------------------------------'
                WHEN 2 THEN
                    CASE
                        WHEN CHARINDEX('Formatted/Non:', column_info) = 1 THEN ''
                        ELSE
                            CASE
                                WHEN SUBSTRING(column_info, CHARINDEX(CHAR(255) COLLATE Latin1_General_Bin2, column_info, CHARINDEX('Non-Formatted:', column_info))+1, 1) = '<' THEN
                                    SUBSTRING(column_info, CHARINDEX(CHAR(255) COLLATE Latin1_General_Bin2, column_info, CHARINDEX('Non-Formatted:', column_info))+1, CHARINDEX('>', column_info, CHARINDEX(CHAR(255) COLLATE Latin1_General_Bin2, column_info, CHARINDEX('Non-Formatted:', column_info))+1) - CHARINDEX(CHAR(255) COLLATE Latin1_General_Bin2, column_info, CHARINDEX('Non-Formatted:', column_info)))
                                ELSE
                                    SUBSTRING(column_info, CHARINDEX(CHAR(255) COLLATE Latin1_General_Bin2, column_info, CHARINDEX('Non-Formatted:', column_info))+1, CHARINDEX(']', column_info, CHARINDEX(CHAR(255) COLLATE Latin1_General_Bin2, column_info, CHARINDEX('Non-Formatted:', column_info))+1) - CHARINDEX(CHAR(255) COLLATE Latin1_General_Bin2, column_info, CHARINDEX('Non-Formatted:', column_info)))
                            END
                    END
                ELSE ''
            END AS unformatted_column_name,
            CASE output_group_order
                WHEN 1 THEN '---------------------------------------'
                WHEN 2 THEN
                    CASE
                        WHEN CHARINDEX('Formatted/Non:', column_info) = 1 THEN ''
                        ELSE
                            CASE
                                WHEN SUBSTRING(column_info, CHARINDEX(CHAR(255) COLLATE Latin1_General_Bin2, column_info, CHARINDEX('Non-Formatted:', column_info))+1, 1) = '<' THEN ''
                                ELSE
                                    SUBSTRING(column_info, CHARINDEX(']', column_info, CHARINDEX('Non-Formatted:', column_info))+2, CHARINDEX('Non-Formatted:', column_info, CHARINDEX(']', column_info)+2) - CHARINDEX(']', column_info)-3)
                            END
                    END
                ELSE ''
            END AS unformatted_column_type,
            CASE output_group_order
                WHEN 1 THEN '----------------------------------------------------------------------------------------------------------------------'
                ELSE REPLACE(token, CHAR(255) COLLATE Latin1_General_Bin2, '')
            END AS [------description-----------------------------------------------------------------------------------------------------]
        FROM output_tokens
        WHERE
            NOT
            (
                output_group_order = 1
                AND output_group = LEN(@outputs)
            )
        ORDER BY
            output_group,
            CASE output_group_order
                WHEN 1 THEN 99
                ELSE output_group_order
            END;

        RETURN;
    END;

    WITH
    a0 AS
    (SELECT 1 AS n UNION ALL SELECT 1),
    a1 AS
    (SELECT 1 AS n FROM a0 AS a CROSS JOIN a0 AS b),
    a2 AS
    (SELECT 1 AS n FROM a1 AS a CROSS JOIN a1 AS b),
    a3 AS
    (SELECT 1 AS n FROM a2 AS a CROSS JOIN a2 AS b),
    a4 AS
    (SELECT 1 AS n FROM a3 AS a CROSS JOIN a3 AS b),
    numbers AS
    (
        SELECT TOP(LEN(@output_column_list))
            ROW_NUMBER() OVER
            (
                ORDER BY (SELECT NULL)
            ) AS number
        FROM a4
        ORDER BY
            number
    ),
    tokens AS
    (
        SELECT
            '|[' +
                SUBSTRING
                (
                    @output_column_list,
                    number + 1,
                    CHARINDEX(']', @output_column_list, number) - number - 1
                ) + '|]' AS token,
            number
        FROM numbers
        WHERE
            SUBSTRING(@output_column_list, number, 1) = '['
    ),
    ordered_columns AS
    (
        SELECT
            x.column_name,
            ROW_NUMBER() OVER
            (
                PARTITION BY
                    x.column_name
                ORDER BY
                    tokens.number,
                    x.default_order
            ) AS r,
            ROW_NUMBER() OVER
            (
                ORDER BY
                    tokens.number,
                    x.default_order
            ) AS s
        FROM tokens
        JOIN
        (
            SELECT '[session_id]' AS column_name, 1 AS default_order
            UNION ALL
            SELECT '[dd hh:mm:ss.mss]', 2
            WHERE
                @format_output IN (1, 2)
            UNION ALL
            SELECT '[dd hh:mm:ss.mss (avg)]', 3
            WHERE
                @format_output IN (1, 2)
                AND @get_avg_time = 1
            UNION ALL
            SELECT '[avg_elapsed_time]', 4
            WHERE
                @format_output = 0
                AND @get_avg_time = 1
            UNION ALL
            SELECT '[physical_io]', 5
            WHERE
                @get_task_info = 2
            UNION ALL
            SELECT '[reads]', 6
            UNION ALL
            SELECT '[physical_reads]', 7
            UNION ALL
            SELECT '[writes]', 8
            UNION ALL
            SELECT '[tempdb_allocations]', 9
            UNION ALL
            SELECT '[tempdb_current]', 10
            UNION ALL
            SELECT '[CPU]', 11
            UNION ALL
            SELECT '[context_switches]', 12
            WHERE
                @get_task_info = 2
            UNION ALL
            SELECT '[used_memory]', 13
            UNION ALL
            SELECT '[max_used_memory]', 14
            WHERE
                @get_memory_info = 1
            UNION ALL
            SELECT '[requested_memory]', 15
            WHERE
                @get_memory_info = 1
            UNION ALL
            SELECT '[granted_memory]', 16
            WHERE
                @get_memory_info = 1
            UNION ALL
            SELECT '[physical_io_delta]', 17
            WHERE
                @delta_interval > 0   
                AND @get_task_info = 2
            UNION ALL
            SELECT '[reads_delta]', 18
            WHERE
                @delta_interval > 0
            UNION ALL
            SELECT '[physical_reads_delta]', 19
            WHERE
                @delta_interval > 0
            UNION ALL
            SELECT '[writes_delta]', 20
            WHERE
                @delta_interval > 0
            UNION ALL
            SELECT '[tempdb_allocations_delta]', 21
            WHERE
                @delta_interval > 0
            UNION ALL
            SELECT '[tempdb_current_delta]', 22
            WHERE
                @delta_interval > 0
            UNION ALL
            SELECT '[CPU_delta]', 23
            WHERE
                @delta_interval > 0
            UNION ALL
            SELECT '[context_switches_delta]', 24
            WHERE
                @delta_interval > 0
                AND @get_task_info = 2
            UNION ALL
            SELECT '[used_memory_delta]', 25
            WHERE
                @delta_interval > 0
            UNION ALL
            SELECT '[max_used_memory_delta]', 26
            WHERE
                @delta_interval > 0
                AND @get_memory_info = 1
            UNION ALL
            SELECT '[tasks]', 27
            WHERE
                @get_task_info = 2
            UNION ALL
            SELECT '[status]', 28
            UNION ALL
            SELECT '[wait_info]', 29
            WHERE
                @get_task_info > 0
                OR @find_block_leaders = 1
            UNION ALL
            SELECT '[locks]', 30
            WHERE
                @get_locks = 1
            UNION ALL
            SELECT '[tran_start_time]', 31
            WHERE
                @get_transaction_info = 1
            UNION ALL
            SELECT '[tran_log_writes]', 32
            WHERE
                @get_transaction_info = 1
            UNION ALL
            SELECT '[implicit_tran]', 33
            WHERE
                @get_transaction_info = 1
            UNION ALL
            SELECT '[open_tran_count]', 34
            UNION ALL
            SELECT '[sql_command]', 35
            WHERE
                @get_outer_command = 1
            UNION ALL
            SELECT '[sql_text]', 36
            UNION ALL
            SELECT '[query_plan]', 37
            WHERE
                @get_plans >= 1
            UNION ALL
            SELECT '[blocking_session_id]', 38
            WHERE
                @get_task_info > 0
                OR @find_block_leaders = 1
            UNION ALL
            SELECT '[blocked_session_count]', 39
            WHERE
                @find_block_leaders = 1
            UNION ALL
            SELECT '[percent_complete]', 40
            UNION ALL
            SELECT '[host_name]', 41
            UNION ALL
            SELECT '[login_name]', 42
            UNION ALL
            SELECT '[database_name]', 43
            UNION ALL
            SELECT '[program_name]', 44
            UNION ALL
            SELECT '[additional_info]', 45
            WHERE
                @get_additional_info = 1
            UNION ALL
            SELECT '[memory_info]', 46
            WHERE
                @get_memory_info = 1
            UNION ALL
            SELECT '[start_time]', 47
            UNION ALL
            SELECT '[login_time]', 48
            UNION ALL
            SELECT '[request_id]', 49
            UNION ALL
            SELECT '[collection_time]', 50
        ) AS x ON
            x.column_name LIKE token ESCAPE '|'
    )
    SELECT
        @output_column_list =
            STUFF
            (
                (
                    SELECT
                        ',' + column_name as [text()]
                    FROM ordered_columns
                    WHERE
                        r = 1
                    ORDER BY
                        s
                    FOR XML
                        PATH('')
                ),
                1,
                1,
                ''
            );
   
    IF COALESCE(RTRIM(@output_column_list), '') = ''
    BEGIN;
        RAISERROR('No valid column matches found in @output_column_list or no columns remain due to selected options.', 16, 1);
        RETURN;
    END;
   
    IF @destination_table <> ''
    BEGIN;
        SET @destination_table =
            --database
            COALESCE(QUOTENAME(PARSENAME(@destination_table, 3)) + '.', '') +
            --schema
            COALESCE(QUOTENAME(PARSENAME(@destination_table, 2)) + '.', '') +
            --table
            COALESCE(QUOTENAME(PARSENAME(@destination_table, 1)), '');
           
        IF COALESCE(RTRIM(@destination_table), '') = ''
        BEGIN;
            RAISERROR('Destination table not properly formatted.', 16, 1);
            RETURN;
        END;
    END;

    WITH
    a0 AS
    (SELECT 1 AS n UNION ALL SELECT 1),
    a1 AS
    (SELECT 1 AS n FROM a0 AS a CROSS JOIN a0 AS b),
    a2 AS
    (SELECT 1 AS n FROM a1 AS a CROSS JOIN a1 AS b),
    a3 AS
    (SELECT 1 AS n FROM a2 AS a CROSS JOIN a2 AS b),
    a4 AS
    (SELECT 1 AS n FROM a3 AS a CROSS JOIN a3 AS b),
    numbers AS
    (
        SELECT TOP(LEN(@sort_order))
            ROW_NUMBER() OVER
            (
                ORDER BY (SELECT NULL)
            ) AS number
        FROM a4
        ORDER BY
            number
    ),
    tokens AS
    (
        SELECT
            '|[' +
                SUBSTRING
                (
                    @sort_order,
                    number + 1,
                    CHARINDEX(']', @sort_order, number) - number - 1
                ) + '|]' AS token,
            SUBSTRING
            (
                @sort_order,
                CHARINDEX(']', @sort_order, number) + 1,
                COALESCE(NULLIF(CHARINDEX('[', @sort_order, CHARINDEX(']', @sort_order, number)), 0), LEN(@sort_order)) - CHARINDEX(']', @sort_order, number)
            ) AS next_chunk,
            number
        FROM numbers
        WHERE
            SUBSTRING(@sort_order, number, 1) = '['
    ),
    ordered_columns AS
    (
        SELECT
            x.column_name +
                CASE
                    WHEN LOWER(tokens.next_chunk) LIKE '%asc%' THEN ' ASC'
                    WHEN LOWER(tokens.next_chunk) LIKE '%desc%' THEN ' DESC'
                    ELSE ''
                END AS column_name,
            ROW_NUMBER() OVER
            (
                PARTITION BY
                    x.column_name
                ORDER BY
                    tokens.number
            ) AS r,
            tokens.number
        FROM tokens
        JOIN
        (
            SELECT '[session_id]' AS column_name
            UNION ALL
            SELECT '[physical_io]'
            UNION ALL
            SELECT '[reads]'
            UNION ALL
            SELECT '[physical_reads]'
            UNION ALL
            SELECT '[writes]'
            UNION ALL
            SELECT '[tempdb_allocations]'
            UNION ALL
            SELECT '[tempdb_current]'
            UNION ALL
            SELECT '[CPU]'
            UNION ALL
            SELECT '[context_switches]'
            UNION ALL
            SELECT '[used_memory]'
            UNION ALL
            SELECT '[max_used_memory]'
            UNION ALL
            SELECT '[requested_memory]'
            UNION ALL
            SELECT '[granted_memory]'
            UNION ALL
            SELECT '[physical_io_delta]'
            UNION ALL
            SELECT '[reads_delta]'
            UNION ALL
            SELECT '[physical_reads_delta]'
            UNION ALL
            SELECT '[writes_delta]'
            UNION ALL
            SELECT '[tempdb_allocations_delta]'
            UNION ALL
            SELECT '[tempdb_current_delta]'
            UNION ALL
            SELECT '[CPU_delta]'
            UNION ALL
            SELECT '[context_switches_delta]'
            UNION ALL
            SELECT '[used_memory_delta]'
            UNION ALL
            SELECT '[max_used_memory_delta]'
            UNION ALL
            SELECT '[tasks]'
            UNION ALL
            SELECT '[tran_start_time]'
            UNION ALL
            SELECT '[open_tran_count]'
            UNION ALL
            SELECT '[blocking_session_id]'
            UNION ALL
            SELECT '[blocked_session_count]'
            UNION ALL
            SELECT '[percent_complete]'
            UNION ALL
            SELECT '[host_name]'
            UNION ALL
            SELECT '[login_name]'
            UNION ALL
            SELECT '[database_name]'
            UNION ALL
            SELECT '[start_time]'
            UNION ALL
            SELECT '[login_time]'
            UNION ALL
            SELECT '[program_name]'
        ) AS x ON
            x.column_name LIKE token ESCAPE '|'
    )
    SELECT
        @sort_order = COALESCE(z.sort_order, '')
    FROM
    (
        SELECT
            STUFF
            (
                (
                    SELECT
                        ',' + column_name as [text()]
                    FROM ordered_columns
                    WHERE
                        r = 1
                    ORDER BY
                        number
                    FOR XML
                        PATH('')
                ),
                1,
                1,
                ''
            ) AS sort_order
    ) AS z;

    CREATE TABLE #sessions
    (
        recursion SMALLINT NOT NULL,
        session_id SMALLINT NOT NULL,
        request_id INT NOT NULL,
        session_number INT NOT NULL,
        elapsed_time INT NOT NULL,
        avg_elapsed_time INT NULL,
        physical_io BIGINT NULL,
        reads BIGINT NULL,
        physical_reads BIGINT NULL,
        writes BIGINT NULL,
        tempdb_allocations BIGINT NULL,
        tempdb_current BIGINT NULL,
        CPU BIGINT NULL,
        thread_CPU_snapshot BIGINT NULL,
        context_switches BIGINT NULL,
        used_memory BIGINT NOT NULL,
        max_used_memory BIGINT NULL,
        requested_memory BIGINT NULL,
        granted_memory BIGINT NULL,
        tasks SMALLINT NULL,
        status VARCHAR(30) NOT NULL,
        wait_info NVARCHAR(4000) NULL,
        locks XML NULL,
        transaction_id BIGINT NULL,
        tran_start_time DATETIME NULL,
        tran_log_writes NVARCHAR(4000) NULL,
        implicit_tran NVARCHAR(3) NULL,
        open_tran_count SMALLINT NULL,
        sql_command XML NULL,
        sql_handle VARBINARY(64) NULL,
        statement_start_offset INT NULL,
        statement_end_offset INT NULL,
        sql_text XML NULL,
        plan_handle VARBINARY(64) NULL,
        query_plan XML NULL,
        blocking_session_id SMALLINT NULL,
        blocked_session_count SMALLINT NULL,
        percent_complete REAL NULL,
        host_name sysname NULL,
        login_name sysname NOT NULL,
        database_name sysname NULL,
        program_name sysname NULL,
        additional_info XML NULL,
        memory_info XML NULL,
        start_time DATETIME NOT NULL,
        login_time DATETIME NULL,
        last_request_start_time DATETIME NULL,
        PRIMARY KEY CLUSTERED (session_id, request_id, recursion) WITH (IGNORE_DUP_KEY = ON),
        UNIQUE NONCLUSTERED (transaction_id, session_id, request_id, recursion) WITH (IGNORE_DUP_KEY = ON)
    );

    IF @return_schema = 0
    BEGIN;
        --Disable unnecessary autostats on the table
        CREATE STATISTICS s_session_id ON #sessions (session_id)
        WITH SAMPLE 0 ROWS, NORECOMPUTE;
        CREATE STATISTICS s_request_id ON #sessions (request_id)
        WITH SAMPLE 0 ROWS, NORECOMPUTE;
        CREATE STATISTICS s_transaction_id ON #sessions (transaction_id)
        WITH SAMPLE 0 ROWS, NORECOMPUTE;
        CREATE STATISTICS s_session_number ON #sessions (session_number)
        WITH SAMPLE 0 ROWS, NORECOMPUTE;
        CREATE STATISTICS s_status ON #sessions (status)
        WITH SAMPLE 0 ROWS, NORECOMPUTE;
        CREATE STATISTICS s_start_time ON #sessions (start_time)
        WITH SAMPLE 0 ROWS, NORECOMPUTE;
        CREATE STATISTICS s_last_request_start_time ON #sessions (last_request_start_time)
        WITH SAMPLE 0 ROWS, NORECOMPUTE;
        CREATE STATISTICS s_recursion ON #sessions (recursion)
        WITH SAMPLE 0 ROWS, NORECOMPUTE;

        DECLARE @recursion SMALLINT;
        SET @recursion =
            CASE @delta_interval
                WHEN 0 THEN 1
                ELSE -1
            END;

        DECLARE @first_collection_ms_ticks BIGINT;
        DECLARE @last_collection_start DATETIME;
        DECLARE @sys_info BIT;
        SET @sys_info = ISNULL(CONVERT(BIT, SIGN(OBJECT_ID('sys.dm_os_sys_info'))), 0);

        --Used for the delta pull
        REDO:;
       
        IF
            @get_locks = 1
            AND @recursion = 1
            AND @output_column_list LIKE '%|[locks|]%' ESCAPE '|'
        BEGIN;
            SELECT
                y.resource_type,
                y.database_name,
                y.object_id,
                y.file_id,
                y.page_type,
                y.hobt_id,
                y.allocation_unit_id,
                y.index_id,
                y.schema_id,
                y.principal_id,
                y.request_mode,
                y.request_status,
                y.session_id,
                y.resource_description,
                y.request_count,
                s.request_id,
                s.start_time,
                CONVERT(sysname, NULL) AS object_name,
                CONVERT(sysname, NULL) AS index_name,
                CONVERT(sysname, NULL) AS schema_name,
                CONVERT(sysname, NULL) AS principal_name,
                CONVERT(NVARCHAR(2048), NULL) AS query_error
            INTO #locks
            FROM
            (
                SELECT
                    sp.spid AS session_id,
                    CASE sp.status
                        WHEN 'sleeping' THEN CONVERT(INT, 0)
                        ELSE sp.request_id
                    END AS request_id,
                    CASE sp.status
                        WHEN 'sleeping' THEN sp.last_batch
                        ELSE COALESCE(req.start_time, sp.last_batch)
                    END AS start_time,
                    sp.dbid
                FROM sys.sysprocesses AS sp
                OUTER APPLY
                (
                    SELECT TOP(1)
                        CASE
                            WHEN
                            (
                                sp.hostprocess > ''
                                OR r.total_elapsed_time < 0
                            ) THEN
                                r.start_time
                            ELSE
                                DATEADD
                                (
                                    ms,
                                    1000 * (DATEPART(ms, DATEADD(second, -(r.total_elapsed_time / 1000), GETDATE())) / 500) - DATEPART(ms, DATEADD(second, -(r.total_elapsed_time / 1000), GETDATE())),
                                    DATEADD(second, -(r.total_elapsed_time / 1000), GETDATE())
                                )
                        END AS start_time
                    FROM sys.dm_exec_requests AS r
                    WHERE
                        r.session_id = sp.spid
                        AND r.request_id = sp.request_id
                ) AS req
                WHERE
                    --Process inclusive filter
                    1 =
                        CASE
                            WHEN @filter <> '' THEN
                                CASE @filter_type
                                    WHEN 'session' THEN
                                        CASE
                                            WHEN
                                                CONVERT(SMALLINT, @filter) = 0
                                                OR sp.spid = CONVERT(SMALLINT, @filter)
                                                    THEN 1
                                            ELSE 0
                                        END
                                    WHEN 'program' THEN
                                        CASE
                                            WHEN sp.program_name LIKE @filter THEN 1
                                            ELSE 0
                                        END
                                    WHEN 'login' THEN
                                        CASE
                                            WHEN sp.loginame LIKE @filter THEN 1
                                            ELSE 0
                                        END
                                    WHEN 'host' THEN
                                        CASE
                                            WHEN sp.hostname LIKE @filter THEN 1
                                            ELSE 0
                                        END
                                    WHEN 'database' THEN
                                        CASE
                                            WHEN DB_NAME(sp.dbid) LIKE @filter THEN 1
                                            ELSE 0
                                        END
                                    ELSE 0
                                END
                            ELSE 1
                        END
                    --Process exclusive filter
                    AND 0 =
                        CASE
                            WHEN @not_filter <> '' THEN
                                CASE @not_filter_type
                                    WHEN 'session' THEN
                                        CASE
                                            WHEN sp.spid = CONVERT(SMALLINT, @not_filter) THEN 1
                                            ELSE 0
                                        END
                                    WHEN 'program' THEN
                                        CASE
                                            WHEN sp.program_name LIKE @not_filter THEN 1
                                            ELSE 0
                                        END
                                    WHEN 'login' THEN
                                        CASE
                                            WHEN sp.loginame LIKE @not_filter THEN 1
                                            ELSE 0
                                        END
                                    WHEN 'host' THEN
                                        CASE
                                            WHEN sp.hostname LIKE @not_filter THEN 1
                                            ELSE 0
                                        END
                                    WHEN 'database' THEN
                                        CASE
                                            WHEN DB_NAME(sp.dbid) LIKE @not_filter THEN 1
                                            ELSE 0
                                        END
                                    ELSE 0
                                END
                            ELSE 0
                        END
                    AND
                    (
                        @show_own_spid = 1
                        OR sp.spid <> @@SPID
                    )
                    AND
                    (
                        @show_system_spids = 1
                        OR sp.hostprocess > ''
                    )
                    AND sp.ecid = 0
            ) AS s
            INNER HASH JOIN
            (
                SELECT
                    x.resource_type,
                    x.database_name,
                    x.object_id,
                    x.file_id,
                    CASE
                        WHEN x.page_no = 1 OR x.page_no % 8088 = 0 THEN 'PFS'
                        WHEN x.page_no = 2 OR x.page_no % 511232 = 0 THEN 'GAM'
                        WHEN x.page_no = 3 OR (x.page_no - 1) % 511232 = 0 THEN 'SGAM'
                        WHEN x.page_no = 6 OR (x.page_no - 6) % 511232 = 0 THEN 'DCM'
                        WHEN x.page_no = 7 OR (x.page_no - 7) % 511232 = 0 THEN 'BCM'
                        WHEN x.page_no IS NOT NULL THEN '*'
                        ELSE NULL
                    END AS page_type,
                    x.hobt_id,
                    x.allocation_unit_id,
                    x.index_id,
                    x.schema_id,
                    x.principal_id,
                    x.request_mode,
                    x.request_status,
                    x.session_id,
                    x.request_id,
                    CASE
                        WHEN COALESCE(x.object_id, x.file_id, x.hobt_id, x.allocation_unit_id, x.index_id, x.schema_id, x.principal_id) IS NULL THEN NULLIF(resource_description, '')
                        ELSE NULL
                    END AS resource_description,
                    COUNT(*) AS request_count
                FROM
                (
                    SELECT
                        tl.resource_type +
                            CASE
                                WHEN tl.resource_subtype = '' THEN ''
                                ELSE '.' + tl.resource_subtype
                            END AS resource_type,
                        COALESCE(DB_NAME(tl.resource_database_id), N'(null)') AS database_name,
                        CONVERT
                        (
                            INT,
                            CASE
                                WHEN tl.resource_type = 'OBJECT' THEN tl.resource_associated_entity_id
                                WHEN tl.resource_description LIKE '%object_id = %' THEN
                                    (
                                        SUBSTRING
                                        (
                                            tl.resource_description,
                                            (CHARINDEX('object_id = ', tl.resource_description) + 12),
                                            COALESCE
                                            (
                                                NULLIF
                                                (
                                                    CHARINDEX(',', tl.resource_description, CHARINDEX('object_id = ', tl.resource_description) + 12),
                                                    0
                                                ),
                                                DATALENGTH(tl.resource_description)+1
                                            ) - (CHARINDEX('object_id = ', tl.resource_description) + 12)
                                        )
                                    )
                                ELSE NULL
                            END
                        ) AS object_id,
                        CONVERT
                        (
                            INT,
                            CASE
                                WHEN tl.resource_type = 'FILE' THEN CONVERT(INT, tl.resource_description)
                                WHEN tl.resource_type IN ('PAGE', 'EXTENT', 'RID') THEN LEFT(tl.resource_description, CHARINDEX(':', tl.resource_description)-1)
                                ELSE NULL
                            END
                        ) AS file_id,
                        CONVERT
                        (
                            INT,
                            CASE
                                WHEN tl.resource_type IN ('PAGE', 'EXTENT', 'RID') THEN
                                    SUBSTRING
                                    (
                                        tl.resource_description,
                                        CHARINDEX(':', tl.resource_description) + 1,
                                        COALESCE
                                        (
                                            NULLIF
                                            (
                                                CHARINDEX(':', tl.resource_description, CHARINDEX(':', tl.resource_description) + 1),
                                                0
                                            ),
                                            DATALENGTH(tl.resource_description)+1
                                        ) - (CHARINDEX(':', tl.resource_description) + 1)
                                    )
                                ELSE NULL
                            END
                        ) AS page_no,
                        CASE
                            WHEN tl.resource_type IN ('PAGE', 'KEY', 'RID', 'HOBT') THEN tl.resource_associated_entity_id
                            ELSE NULL
                        END AS hobt_id,
                        CASE
                            WHEN tl.resource_type = 'ALLOCATION_UNIT' THEN tl.resource_associated_entity_id
                            ELSE NULL
                        END AS allocation_unit_id,
                        CONVERT
                        (
                            INT,
                            CASE
                                WHEN
                                    /*TODO: Deal with server principals*/
                                    tl.resource_subtype <> 'SERVER_PRINCIPAL'
                                    AND tl.resource_description LIKE '%index_id or stats_id = %' THEN
                                    (
                                        SUBSTRING
                                        (
                                            tl.resource_description,
                                            (CHARINDEX('index_id or stats_id = ', tl.resource_description) + 23),
                                            COALESCE
                                            (
                                                NULLIF
                                                (
                                                    CHARINDEX(',', tl.resource_description, CHARINDEX('index_id or stats_id = ', tl.resource_description) + 23),
                                                    0
                                                ),
                                                DATALENGTH(tl.resource_description)+1
                                            ) - (CHARINDEX('index_id or stats_id = ', tl.resource_description) + 23)
                                        )
                                    )
                                ELSE NULL
                            END
                        ) AS index_id,
                        CONVERT
                        (
                            INT,
                            CASE
                                WHEN tl.resource_description LIKE '%schema_id = %' THEN
                                    (
                                        SUBSTRING
                                        (
                                            tl.resource_description,
                                            (CHARINDEX('schema_id = ', tl.resource_description) + 12),
                                            COALESCE
                                            (
                                                NULLIF
                                                (
                                                    CHARINDEX(',', tl.resource_description, CHARINDEX('schema_id = ', tl.resource_description) + 12),
                                                    0
                                                ),
                                                DATALENGTH(tl.resource_description)+1
                                            ) - (CHARINDEX('schema_id = ', tl.resource_description) + 12)
                                        )
                                    )
                                ELSE NULL
                            END
                        ) AS schema_id,
                        CONVERT
                        (
                            INT,
                            CASE
                                WHEN tl.resource_description LIKE '%principal_id = %' THEN
                                    (
                                        SUBSTRING
                                        (
                                            tl.resource_description,
                                            (CHARINDEX('principal_id = ', tl.resource_description) + 15),
                                            COALESCE
                                            (
                                                NULLIF
                                                (
                                                    CHARINDEX(',', tl.resource_description, CHARINDEX('principal_id = ', tl.resource_description) + 15),
                                                    0
                                                ),
                                                DATALENGTH(tl.resource_description)+1
                                            ) - (CHARINDEX('principal_id = ', tl.resource_description) + 15)
                                        )
                                    )
                                ELSE NULL
                            END
                        ) AS principal_id,
                        tl.request_mode,
                        tl.request_status,
                        tl.request_session_id AS session_id,
                        tl.request_request_id AS request_id,

                        /*TODO: Applocks, other resource_descriptions*/
                        RTRIM(tl.resource_description) AS resource_description,
                        tl.resource_associated_entity_id
                        /*********************************************/
                    FROM
                    (
                        SELECT
                            request_session_id,
                            CONVERT(VARCHAR(120), resource_type) COLLATE Latin1_General_Bin2 AS resource_type,
                            CONVERT(VARCHAR(120), resource_subtype) COLLATE Latin1_General_Bin2 AS resource_subtype,
                            resource_database_id,
                            CONVERT(VARCHAR(512), resource_description) COLLATE Latin1_General_Bin2 AS resource_description,
                            resource_associated_entity_id,
                            CONVERT(VARCHAR(120), request_mode) COLLATE Latin1_General_Bin2 AS request_mode,
                            CONVERT(VARCHAR(120), request_status) COLLATE Latin1_General_Bin2 AS request_status,
                            request_request_id
                        FROM sys.dm_tran_locks
                    ) AS tl
                ) AS x
                GROUP BY
                    x.resource_type,
                    x.database_name,
                    x.object_id,
                    x.file_id,
                    CASE
                        WHEN x.page_no = 1 OR x.page_no % 8088 = 0 THEN 'PFS'
                        WHEN x.page_no = 2 OR x.page_no % 511232 = 0 THEN 'GAM'
                        WHEN x.page_no = 3 OR (x.page_no - 1) % 511232 = 0 THEN 'SGAM'
                        WHEN x.page_no = 6 OR (x.page_no - 6) % 511232 = 0 THEN 'DCM'
                        WHEN x.page_no = 7 OR (x.page_no - 7) % 511232 = 0 THEN 'BCM'
                        WHEN x.page_no IS NOT NULL THEN '*'
                        ELSE NULL
                    END,
                    x.hobt_id,
                    x.allocation_unit_id,
                    x.index_id,
                    x.schema_id,
                    x.principal_id,
                    x.request_mode,
                    x.request_status,
                    x.session_id,
                    x.request_id,
                    CASE
                        WHEN COALESCE(x.object_id, x.file_id, x.hobt_id, x.allocation_unit_id, x.index_id, x.schema_id, x.principal_id) IS NULL THEN NULLIF(resource_description, '')
                        ELSE NULL
                    END
            ) AS y ON
                y.session_id = s.session_id
                AND y.request_id = s.request_id
            OPTION (HASH GROUP);

            --Disable unnecessary autostats on the table
            CREATE STATISTICS s_database_name ON #locks (database_name)
            WITH SAMPLE 0 ROWS, NORECOMPUTE;
            CREATE STATISTICS s_object_id ON #locks (object_id)
            WITH SAMPLE 0 ROWS, NORECOMPUTE;
            CREATE STATISTICS s_hobt_id ON #locks (hobt_id)
            WITH SAMPLE 0 ROWS, NORECOMPUTE;
            CREATE STATISTICS s_allocation_unit_id ON #locks (allocation_unit_id)
            WITH SAMPLE 0 ROWS, NORECOMPUTE;
            CREATE STATISTICS s_index_id ON #locks (index_id)
            WITH SAMPLE 0 ROWS, NORECOMPUTE;
            CREATE STATISTICS s_schema_id ON #locks (schema_id)
            WITH SAMPLE 0 ROWS, NORECOMPUTE;
            CREATE STATISTICS s_principal_id ON #locks (principal_id)
            WITH SAMPLE 0 ROWS, NORECOMPUTE;
            CREATE STATISTICS s_request_id ON #locks (request_id)
            WITH SAMPLE 0 ROWS, NORECOMPUTE;
            CREATE STATISTICS s_start_time ON #locks (start_time)
            WITH SAMPLE 0 ROWS, NORECOMPUTE;
            CREATE STATISTICS s_resource_type ON #locks (resource_type)
            WITH SAMPLE 0 ROWS, NORECOMPUTE;
            CREATE STATISTICS s_object_name ON #locks (object_name)
            WITH SAMPLE 0 ROWS, NORECOMPUTE;
            CREATE STATISTICS s_schema_name ON #locks (schema_name)
            WITH SAMPLE 0 ROWS, NORECOMPUTE;
            CREATE STATISTICS s_page_type ON #locks (page_type)
            WITH SAMPLE 0 ROWS, NORECOMPUTE;
            CREATE STATISTICS s_request_mode ON #locks (request_mode)
            WITH SAMPLE 0 ROWS, NORECOMPUTE;
            CREATE STATISTICS s_request_status ON #locks (request_status)
            WITH SAMPLE 0 ROWS, NORECOMPUTE;
            CREATE STATISTICS s_resource_description ON #locks (resource_description)
            WITH SAMPLE 0 ROWS, NORECOMPUTE;
            CREATE STATISTICS s_index_name ON #locks (index_name)
            WITH SAMPLE 0 ROWS, NORECOMPUTE;
            CREATE STATISTICS s_principal_name ON #locks (principal_name)
            WITH SAMPLE 0 ROWS, NORECOMPUTE;
        END;
       
        DECLARE
            @sql VARCHAR(MAX),
            @sql_n NVARCHAR(MAX),
            @core_session_join VARCHAR(MAX);

        SET @core_session_join =
                '@sessions AS sp
                LEFT OUTER LOOP JOIN sys.dm_exec_sessions AS s ON
                    s.session_id = sp.session_id
                    AND s.login_time = sp.login_time
                LEFT OUTER LOOP JOIN sys.dm_exec_requests AS r ON
                    sp.status <> ''sleeping''
                    AND r.session_id = sp.session_id
                    AND r.request_id = sp.request_id
                    AND
                    (
                        (
                            s.is_user_process = 0
                            AND sp.is_user_process = 0
                        )
                        OR
                        (
                            r.start_time = s.last_request_start_time
                            AND s.last_request_end_time <= sp.last_request_end_time
                        )
                    ) ';

        SET @sql =
            CONVERT(VARCHAR(MAX), '') +
            'DECLARE @blocker BIT;
            SET @blocker = 0;
            DECLARE @i INT;
            SET @i = 2147483647;

            DECLARE @sessions TABLE
            (
                session_id SMALLINT NOT NULL,
                request_id INT NOT NULL,
                login_time DATETIME,
                last_request_end_time DATETIME,
                status VARCHAR(30),
                statement_start_offset INT,
                statement_end_offset INT,
                sql_handle BINARY(20),
                host_name NVARCHAR(128),
                login_name NVARCHAR(128),
                program_name NVARCHAR(128),
                database_id SMALLINT,
                memory_usage INT,
                open_tran_count SMALLINT,
                ' +
                CASE
                    WHEN
                    (
                        @get_task_info <> 0
                        OR @find_block_leaders = 1
                    ) THEN
                        'wait_type NVARCHAR(32),
                        wait_resource NVARCHAR(256),
                        wait_time BIGINT,
                        '
                    ELSE
                        ''
                END +
                'blocked SMALLINT,
                is_user_process BIT,
                cmd VARCHAR(32),
                PRIMARY KEY CLUSTERED (session_id, request_id) WITH (IGNORE_DUP_KEY = ON)
            );

            DECLARE @blockers TABLE
            (
                session_id INT NOT NULL PRIMARY KEY WITH (IGNORE_DUP_KEY = ON)
            );

            BLOCKERS:;

            INSERT @sessions
            (
                session_id,
                request_id,
                login_time,
                last_request_end_time,
                status,
                statement_start_offset,
                statement_end_offset,
                sql_handle,
                host_name,
                login_name,
                program_name,
                database_id,
                memory_usage,
                open_tran_count,
                ' +
                CASE
                    WHEN
                    (
                        @get_task_info <> 0
                        OR @find_block_leaders = 1
                    ) THEN
                        'wait_type,
                        wait_resource,
                        wait_time,
                        '
                    ELSE
                        ''
                END +
                'blocked,
                is_user_process,
                cmd
            )
            SELECT TOP(@i)
                spy.session_id,
                spy.request_id,
                spy.login_time,
                spy.last_request_end_time,
                spy.status,
                spy.statement_start_offset,
                spy.statement_end_offset,
                spy.sql_handle,
                spy.host_name,
                spy.login_name,
                spy.program_name,
                spy.database_id,
                spy.memory_usage,
                spy.open_tran_count,
                ' +
                CASE
                    WHEN
                    (
                        @get_task_info <> 0
                        OR @find_block_leaders = 1
                    ) THEN
                        'spy.wait_type,
                        CASE
                            WHEN
                                spy.wait_type LIKE N''PAGE%LATCH_%''
                                OR spy.wait_type IN (N''CXPACKET'', N''CXCONSUMER'', N''CXSYNC_PORT'', N''CXSYNC_CONSUMER'')
                                OR spy.wait_type LIKE N''LATCH[_]%''
                                OR spy.wait_type = N''OLEDB'' THEN
                                    spy.wait_resource
                            ELSE
                                NULL
                        END AS wait_resource,
                        spy.wait_time,
                        '
                    ELSE
                        ''
                END +
                'spy.blocked,
                spy.is_user_process,
                spy.cmd
            FROM
            (
                SELECT TOP(@i)
                    spx.*,
                    ' +
                    CASE
                        WHEN
                        (
                            @get_task_info <> 0
                            OR @find_block_leaders = 1
                        ) THEN
                            'ROW_NUMBER() OVER
                            (
                                PARTITION BY
                                    spx.session_id,
                                    spx.request_id
                                ORDER BY
                                    CASE
                                        WHEN spx.wait_type LIKE N''LCK[_]%'' THEN
                                            1
                                        ELSE
                                            99
                                    END,
                                    spx.wait_time DESC,
                                    spx.blocked DESC
                            ) AS r
                            '
                        ELSE
                            '1 AS r
                            '
                    END +
                'FROM
                (
                    SELECT TOP(@i)
                        sp0.session_id,
                        sp0.request_id,
                        sp0.login_time,
                        sp0.last_request_end_time,
                        LOWER(sp0.status) AS status,
                        CASE
                            WHEN sp0.cmd = ''CREATE INDEX'' THEN
                                0
                            ELSE
                                sp0.stmt_start
                        END AS statement_start_offset,
                        CASE
                            WHEN sp0.cmd = N''CREATE INDEX'' THEN
                                -1
                            ELSE
                                COALESCE(NULLIF(sp0.stmt_end, 0), -1)
                        END AS statement_end_offset,
                        sp0.sql_handle,
                        sp0.host_name,
                        sp0.login_name,
                        sp0.program_name,
                        sp0.database_id,
                        sp0.memory_usage,
                        sp0.open_tran_count,
                        ' +
                        CASE
                            WHEN
                            (
                                @get_task_info <> 0
                                OR @find_block_leaders = 1
                            ) THEN
                                'CASE
                                    WHEN sp0.wait_time > 0 AND sp0.wait_type NOT IN (N''CXPACKET'', N''CXCONSUMER'', N''CXSYNC_PORT'', N''CXSYNC_CONSUMER'') THEN
                                        sp0.wait_type
                                    ELSE
                                        NULL
                                END AS wait_type,
                                CASE
                                    WHEN sp0.wait_time > 0 AND sp0.wait_type NOT IN (N''CXPACKET'', N''CXCONSUMER'', N''CXSYNC_PORT'', N''CXSYNC_CONSUMER'') THEN
                                        sp0.wait_resource
                                    ELSE
                                        NULL
                                END AS wait_resource,
                                CASE
                                    WHEN sp0.wait_type NOT IN (N''CXPACKET'', N''CXCONSUMER'', N''CXSYNC_PORT'', N''CXSYNC_CONSUMER'') THEN
                                        sp0.wait_time
                                    ELSE
                                        0
                                END AS wait_time,
                                '
                            ELSE
                                ''
                        END +
                        'sp0.blocked,
                        sp0.is_user_process,
                        sp0.cmd
                    FROM
                    (
                        SELECT TOP(@i)
                            sp1.session_id,
                            sp1.request_id,
                            sp1.login_time,
                            sp1.last_request_end_time,
                            sp1.status,
                            sp1.cmd,
                            sp1.stmt_start,
                            sp1.stmt_end,
                            MAX(NULLIF(sp1.sql_handle, 0x00)) OVER (PARTITION BY sp1.session_id, sp1.request_id) AS sql_handle,
                            sp1.host_name,
                            MAX(sp1.login_name) OVER (PARTITION BY sp1.session_id, sp1.request_id) AS login_name,
                            sp1.program_name,
                            sp1.database_id,
                            MAX(sp1.memory_usage)  OVER (PARTITION BY sp1.session_id, sp1.request_id) AS memory_usage,
                            MAX(sp1.open_tran_count)  OVER (PARTITION BY sp1.session_id, sp1.request_id) AS open_tran_count,
                            sp1.wait_type,
                            sp1.wait_resource,
                            sp1.wait_time,
                            sp1.blocked,
                            sp1.hostprocess,
                            sp1.is_user_process
                        FROM
                        (
                            SELECT TOP(@i)
                                sp2.spid AS session_id,
                                v2.request_id,
                                MAX(sp2.login_time) AS login_time,
                                MAX(sp2.last_batch) AS last_request_end_time,
                                MAX(CONVERT(VARCHAR(30), RTRIM(sp2.status)) COLLATE Latin1_General_Bin2) AS status,
                                MAX(CONVERT(VARCHAR(32), RTRIM(sp2.cmd)) COLLATE Latin1_General_Bin2) AS cmd,
                                MAX(sp2.stmt_start) AS stmt_start,
                                MAX(sp2.stmt_end) AS stmt_end,
                                MAX(sp2.sql_handle) AS sql_handle,
                                MAX(CONVERT(sysname, RTRIM(sp2.hostname)) COLLATE SQL_Latin1_General_CP1_CI_AS) AS host_name,
                                MAX(CONVERT(sysname, RTRIM(sp2.loginame)) COLLATE SQL_Latin1_General_CP1_CI_AS) AS login_name,
                                MAX
                                (
                                    CASE
                                        WHEN blk.queue_id IS NOT NULL THEN
                                            N''Service Broker
                                                database_id: '' + CONVERT(NVARCHAR, blk.database_id) +
                                                N'' queue_id: '' + CONVERT(NVARCHAR, blk.queue_id)
                                        ELSE
                                            CONVERT
                                            (
                                                sysname,
                                                RTRIM(sp2.program_name)
                                            )
                                    END COLLATE SQL_Latin1_General_CP1_CI_AS
                                ) AS program_name,
                                MAX(sp2.dbid) AS database_id,
                                MAX(sp2.memusage) AS memory_usage,
                                MAX(sp2.open_tran) AS open_tran_count,
                                RTRIM(sp2.lastwaittype) AS wait_type,
                                v2.wait_resource,
                                MAX(sp2.waittime) AS wait_time,
                                COALESCE(NULLIF(sp2.blocked, sp2.spid), 0) AS blocked,
                                MAX
                                (
                                    CASE
                                        WHEN blk.session_id = sp2.spid THEN
                                            ''blocker''
                                        ELSE
                                            RTRIM(sp2.hostprocess)
                                    END
                                ) AS hostprocess,
                                CONVERT
                                (
                                    BIT,
                                    MAX
                                    (
                                        CASE
                                            WHEN sp2.hostprocess > '''' THEN
                                                1
                                            ELSE
                                                0
                                        END
                                    )
                                ) AS is_user_process
                            FROM
                            (
                                SELECT TOP(@i)
                                    session_id,
                                    CONVERT(INT, NULL) AS queue_id,
                                    CONVERT(INT, NULL) AS database_id
                                FROM @blockers

                                UNION ALL

                                SELECT TOP(@i)
                                    CONVERT(SMALLINT, 0),
                                    CONVERT(INT, NULL) AS queue_id,
                                    CONVERT(INT, NULL) AS database_id
                                WHERE
                                    @blocker = 0

                                UNION ALL

                                SELECT TOP(@i)
                                    CONVERT(SMALLINT, spid),
                                    queue_id,
                                    database_id
                                FROM sys.dm_broker_activated_tasks
                                WHERE
                                    @blocker = 0
                            ) AS blk
                            INNER JOIN sys.sysprocesses AS sp2 ON
                                sp2.spid = blk.session_id
                                OR
                                (
                                    blk.session_id = 0
                                    AND @blocker = 0
                                )
                            CROSS APPLY
                            (
                                SELECT
                                    CASE sp2.status
                                        WHEN ''sleeping'' THEN
                                            CONVERT(INT, 0)
                                        ELSE
                                            sp2.request_id
                                    END AS request_id,
                                    RTRIM
                                    (
                                        LEFT
                                        (
                                            sp2.waitresource COLLATE Latin1_General_Bin2,
                                            ISNULL(NULLIF(CHARINDEX('' (LATCH '', sp2.waitresource COLLATE Latin1_General_Bin2) - 1, -1), 256)
                                        )
                                    ) AS wait_resource
                            ) AS v2
                            ' +
                            CASE
                                WHEN
                                (
                                    @get_task_info = 0
                                    AND @find_block_leaders = 0
                                ) THEN
                                    'WHERE
                                        sp2.ecid = 0
                                    '
                                ELSE
                                    ''
                            END +
                            'GROUP BY
                                sp2.spid,
                                v2.request_id,
                                RTRIM(sp2.lastwaittype),
                                v2.wait_resource,
                                COALESCE(NULLIF(sp2.blocked, sp2.spid), 0)
                        ) AS sp1
                    ) AS sp0
                    WHERE
                        @blocker = 1
                        OR
                        (1=1
                        ' +
                            --inclusive filter
                            CASE
                                WHEN @filter <> '' THEN
                                    CASE @filter_type
                                        WHEN 'session' THEN
                                            CASE
                                                WHEN CONVERT(SMALLINT, @filter) <> 0 THEN
                                                    'AND sp0.session_id = CONVERT(SMALLINT, @filter)
                                                    '
                                                ELSE
                                                    ''
                                            END
                                        WHEN 'program' THEN
                                            'AND sp0.program_name LIKE @filter
                                            '
                                        WHEN 'login' THEN
                                            'AND sp0.login_name LIKE @filter
                                            '
                                        WHEN 'host' THEN
                                            'AND sp0.host_name LIKE @filter
                                            '
                                        WHEN 'database' THEN
                                            'AND DB_NAME(sp0.database_id) LIKE @filter
                                            '
                                        ELSE
                                            ''
                                    END
                                ELSE
                                    ''
                            END +
                            --exclusive filter
                            CASE
                                WHEN @not_filter <> '' THEN
                                    CASE @not_filter_type
                                        WHEN 'session' THEN
                                            CASE
                                                WHEN CONVERT(SMALLINT, @not_filter) <> 0 THEN
                                                    'AND sp0.session_id <> CONVERT(SMALLINT, @not_filter)
                                                    '
                                                ELSE
                                                    ''
                                            END
                                        WHEN 'program' THEN
                                            'AND sp0.program_name NOT LIKE @not_filter
                                            '
                                        WHEN 'login' THEN
                                            'AND sp0.login_name NOT LIKE @not_filter
                                            '
                                        WHEN 'host' THEN
                                            'AND sp0.host_name NOT LIKE @not_filter
                                            '
                                        WHEN 'database' THEN
                                            'AND DB_NAME(sp0.database_id) NOT LIKE @not_filter
                                            '
                                        ELSE
                                            ''
                                    END
                                ELSE
                                    ''
                            END +
                            CASE @show_own_spid
                                WHEN 1 THEN
                                    ''
                                ELSE
                                    'AND sp0.session_id <> @@spid
                                    '
                            END +
                            CASE
                                WHEN @show_system_spids = 0 THEN
                                    'AND sp0.hostprocess > ''''
                                    '
                                ELSE
                                    ''
                            END +
                            CASE @show_sleeping_spids
                                WHEN 0 THEN
                                    'AND sp0.status <> ''sleeping''
                                    '
                                WHEN 1 THEN
                                    'AND
                                    (
                                        sp0.status <> ''sleeping''
                                        OR sp0.open_tran_count > 0
                                    )
                                    '
                                ELSE
                                    ''
                            END +
                        ')
                ) AS spx
            ) AS spy
            WHERE
                spy.r = 1;
            ' +
            CASE @recursion
                WHEN 1 THEN
                    'IF @@ROWCOUNT > 0
                    BEGIN;
                        INSERT @blockers
                        (
                            session_id
                        )
                        SELECT TOP(@i)
                            blocked
                        FROM @sessions
                        WHERE
                            NULLIF(blocked, 0) IS NOT NULL

                        EXCEPT

                        SELECT TOP(@i)
                            session_id
                        FROM @sessions;
                        ' +

                        CASE
                            WHEN
                            (
                                @get_task_info > 0
                                OR @find_block_leaders = 1
                            ) THEN
                                'IF @@ROWCOUNT > 0
                                BEGIN;
                                    SET @blocker = 1;
                                    GOTO BLOCKERS;
                                END;
                                '
                            ELSE
                                ''
                        END +
                    'END;
                    '
                ELSE
                    ''
            END +
            'SELECT TOP(@i)
                @recursion AS recursion,
                x.session_id,
                x.request_id,
                DENSE_RANK() OVER
                (
                    ORDER BY
                        x.session_id
                ) AS session_number,
                ' +
                CASE
                    WHEN @output_column_list LIKE '%|[dd hh:mm:ss.mss|]%' ESCAPE '|' THEN
                        'x.elapsed_time '
                    ELSE
                        '0 '
                END +
                    'AS elapsed_time,
                    ' +
                CASE
                    WHEN
                        (
                            @output_column_list LIKE '%|[dd hh:mm:ss.mss (avg)|]%' ESCAPE '|' OR
                            @output_column_list LIKE '%|[avg_elapsed_time|]%' ESCAPE '|'
                        )
                        AND @recursion = 1
                            THEN
                                'x.avg_elapsed_time / 1000 '
                    ELSE
                        'NULL '
                END +
                    'AS avg_elapsed_time,
                    ' +
                CASE
                    WHEN
                        @output_column_list LIKE '%|[physical_io|]%' ESCAPE '|'
                        OR @output_column_list LIKE '%|[physical_io_delta|]%' ESCAPE '|'
                            THEN
                                'x.physical_io '
                    ELSE
                        'NULL '
                END +
                    'AS physical_io,
                    ' +
                CASE
                    WHEN
                        @output_column_list LIKE '%|[reads|]%' ESCAPE '|'
                        OR @output_column_list LIKE '%|[reads_delta|]%' ESCAPE '|'
                            THEN
                                'x.reads '
                    ELSE
                        '0 '
                END +
                    'AS reads,
                    ' +
                CASE
                    WHEN
                        @output_column_list LIKE '%|[physical_reads|]%' ESCAPE '|'
                        OR @output_column_list LIKE '%|[physical_reads_delta|]%' ESCAPE '|'
                            THEN
                                'x.physical_reads '
                    ELSE
                        '0 '
                END +
                    'AS physical_reads,
                    ' +
                CASE
                    WHEN
                        @output_column_list LIKE '%|[writes|]%' ESCAPE '|'
                        OR @output_column_list LIKE '%|[writes_delta|]%' ESCAPE '|'
                            THEN
                                'x.writes '
                    ELSE
                        '0 '
                END +
                    'AS writes,
                    ' +
                CASE
                    WHEN
                        @output_column_list LIKE '%|[tempdb_allocations|]%' ESCAPE '|'
                        OR @output_column_list LIKE '%|[tempdb_allocations_delta|]%' ESCAPE '|'
                            THEN
                                'x.tempdb_allocations '
                    ELSE
                        '0 '
                END +
                    'AS tempdb_allocations,
                    ' +
                CASE
                    WHEN
                        @output_column_list LIKE '%|[tempdb_current|]%' ESCAPE '|'
                        OR @output_column_list LIKE '%|[tempdb_current_delta|]%' ESCAPE '|'
                            THEN
                                'x.tempdb_current '
                    ELSE
                        '0 '
                END +
                    'AS tempdb_current,
                    ' +
                CASE
                    WHEN
                        @output_column_list LIKE '%|[CPU|]%' ESCAPE '|'
                        OR @output_column_list LIKE '%|[CPU_delta|]%' ESCAPE '|'
                            THEN
                                'x.CPU '
                    ELSE
                        '0 '
                END +
                    'AS CPU,
                    ' +
                CASE
                    WHEN
                        @output_column_list LIKE '%|[CPU_delta|]%' ESCAPE '|'
                        AND @get_task_info = 2
                        AND @sys_info = 1
                            THEN
                                'x.thread_CPU_snapshot '
                    ELSE
                        '0 '
                END +
                    'AS thread_CPU_snapshot,
                    ' +
                CASE
                    WHEN
                        @output_column_list LIKE '%|[context_switches|]%' ESCAPE '|'
                        OR @output_column_list LIKE '%|[context_switches_delta|]%' ESCAPE '|'
                            THEN
                                'x.context_switches '
                    ELSE
                        'NULL '
                END +
                    'AS context_switches,
                    ' +
                CASE
                    WHEN
                        @output_column_list LIKE '%|[used_memory|]%' ESCAPE '|'
                        OR @output_column_list LIKE '%|[used_memory_delta|]%' ESCAPE '|'
                            THEN
                                CASE
                                    WHEN @get_memory_info = 1 THEN
                                        'COALESCE(x.mg_used_memory_kb / 8, x.used_memory) '
                                    ELSE
                                        'x.used_memory '
                                END
                    ELSE
                        '0 '
                END +
                    'AS used_memory,
                    ' +
                CASE
                    WHEN
                        @output_column_list LIKE '%|[max_used_memory|]%' ESCAPE '|'
                        OR @output_column_list LIKE '%|[max_used_memory_delta|]%' ESCAPE '|'
                            THEN
                                'x.max_used_memory_kb / 8 '
                    ELSE
                        '0 '
                END +
                    'AS max_used_memory,
                    ' +
                CASE
                    WHEN
                        @output_column_list LIKE '%|[requested_memory|]%' ESCAPE '|'
                            THEN
                                'x.requested_memory_kb / 8 '
                    ELSE
                        '0 '
                END +
                    'AS requested_memory,
                    ' +
                CASE
                    WHEN
                        @output_column_list LIKE '%|[granted_memory|]%' ESCAPE '|'
                            THEN
                                'x.mg_granted_memory_kb / 8 '
                    ELSE
                        '0 '
                END +
                    'AS granted_memory,
                    ' +
                CASE
                    WHEN
                        @output_column_list LIKE '%|[tasks|]%' ESCAPE '|'
                        AND @recursion = 1
                            THEN
                                'x.tasks '
                    ELSE
                        'NULL '
                END +
                    'AS tasks,
                    ' +
                CASE
                    WHEN
                        (
                            @output_column_list LIKE '%|[status|]%' ESCAPE '|'
                            OR @output_column_list LIKE '%|[sql_command|]%' ESCAPE '|'
                        )
                        AND @recursion = 1
                            THEN
                                'x.status '
                    ELSE
                        ''''' '
                END +
                    'AS status,
                    ' +
                CASE
                    WHEN
                        @output_column_list LIKE '%|[wait_info|]%' ESCAPE '|'
                        AND @recursion = 1
                            THEN
                                CASE @get_task_info
                                    WHEN 2 THEN
                                        'COALESCE(x.task_wait_info, x.sys_wait_info) '
                                    ELSE
                                        'x.sys_wait_info '
                                END
                    ELSE
                        'NULL '
                END +
                    'AS wait_info,
                    ' +
                CASE
                    WHEN
                        (
                            @output_column_list LIKE '%|[tran_start_time|]%' ESCAPE '|'
                            OR @output_column_list LIKE '%|[tran_log_writes|]%' ESCAPE '|'
                        )
                        AND @recursion = 1
                            THEN
                                'x.transaction_id '
                    ELSE
                        'NULL '
                END +
                    'AS transaction_id,
                    ' +
                CASE
                    WHEN
                        @output_column_list LIKE '%|[open_tran_count|]%' ESCAPE '|'
                        AND @recursion = 1
                            THEN
                                'x.open_tran_count '
                    ELSE
                        'NULL '
                END +
                    'AS open_tran_count,
                    ' +
                CASE
                    WHEN
                        @output_column_list LIKE '%|[sql_text|]%' ESCAPE '|'
                        AND @recursion = 1
                            THEN
                                'x.sql_handle '
                    ELSE
                        'NULL '
                END +
                    'AS sql_handle,
                    ' +
                CASE
                    WHEN
                        (
                            @output_column_list LIKE '%|[sql_text|]%' ESCAPE '|'
                            OR @output_column_list LIKE '%|[query_plan|]%' ESCAPE '|'
                        )
                        AND @recursion = 1
                            THEN
                                'x.statement_start_offset '
                    ELSE
                        'NULL '
                END +
                    'AS statement_start_offset,
                    ' +
                CASE
                    WHEN
                        (
                            @output_column_list LIKE '%|[sql_text|]%' ESCAPE '|'
                            OR @output_column_list LIKE '%|[query_plan|]%' ESCAPE '|'
                        )
                        AND @recursion = 1
                            THEN
                                'x.statement_end_offset '
                    ELSE
                        'NULL '
                END +
                    'AS statement_end_offset,
                    ' +
                'NULL AS sql_text,
                    ' +
                CASE
                    WHEN
                        @output_column_list LIKE '%|[query_plan|]%' ESCAPE '|'
                        AND @recursion = 1
                            THEN
                                'x.plan_handle '
                    ELSE
                        'NULL '
                END +
                    'AS plan_handle,
                    ' +
                CASE
                    WHEN
                        @output_column_list LIKE '%|[blocking_session_id|]%' ESCAPE '|' OR @find_block_leaders = 1
                        AND @recursion = 1
                            THEN
                                'NULLIF(x.blocking_session_id, 0) '
                    ELSE
                        'NULL '
                END +
                    'AS blocking_session_id,
                    ' +
                CASE
                    WHEN
                        @output_column_list LIKE '%|[percent_complete|]%' ESCAPE '|'
                        AND @recursion = 1
                            THEN
                                'x.percent_complete '
                    ELSE
                        'NULL '
                END +
                    'AS percent_complete,
                    ' +
                CASE
                    WHEN
                        @output_column_list LIKE '%|[host_name|]%' ESCAPE '|'
                        AND @recursion = 1
                            THEN
                                'x.host_name '
                    ELSE
                        ''''' '
                END +
                    'AS host_name,
                    ' +
                CASE
                    WHEN
                        @output_column_list LIKE '%|[login_name|]%' ESCAPE '|'
                        AND @recursion = 1
                            THEN
                                'x.login_name '
                    ELSE
                        ''''' '
                END +
                    'AS login_name,
                    ' +
                CASE
                    WHEN
                        @output_column_list LIKE '%|[database_name|]%' ESCAPE '|'
                        AND @recursion = 1
                            THEN
                                'DB_NAME(x.database_id) '
                    ELSE
                        'NULL '
                END +
                    'AS database_name,
                    ' +
                CASE
                    WHEN
                        @output_column_list LIKE '%|[program_name|]%' ESCAPE '|'
                        AND @recursion = 1
                            THEN
                                'x.program_name '
                    ELSE
                        ''''' '
                END +
                    'AS program_name,
                    ' +
                CASE
                    WHEN
                        @output_column_list LIKE '%|[additional_info|]%' ESCAPE '|'
                        AND @recursion = 1
                            THEN
                                '(
                                    SELECT TOP(@i)
                                        x.text_size,
                                        x.language,
                                        x.date_format,
                                        x.date_first,
                                        CASE x.quoted_identifier
                                            WHEN 0 THEN ''OFF''
                                            WHEN 1 THEN ''ON''
                                        END AS quoted_identifier,
                                        CASE x.arithabort
                                            WHEN 0 THEN ''OFF''
                                            WHEN 1 THEN ''ON''
                                        END AS arithabort,
                                        CASE x.ansi_null_dflt_on
                                            WHEN 0 THEN ''OFF''
                                            WHEN 1 THEN ''ON''
                                        END AS ansi_null_dflt_on,
                                        CASE x.ansi_defaults
                                            WHEN 0 THEN ''OFF''
                                            WHEN 1 THEN ''ON''
                                        END AS ansi_defaults,
                                        CASE x.ansi_warnings
                                            WHEN 0 THEN ''OFF''
                                            WHEN 1 THEN ''ON''
                                        END AS ansi_warnings,
                                        CASE x.ansi_padding
                                            WHEN 0 THEN ''OFF''
                                            WHEN 1 THEN ''ON''
                                        END AS ansi_padding,
                                        CASE ansi_nulls
                                            WHEN 0 THEN ''OFF''
                                            WHEN 1 THEN ''ON''
                                        END AS ansi_nulls,
                                        CASE x.concat_null_yields_null
                                            WHEN 0 THEN ''OFF''
                                            WHEN 1 THEN ''ON''
                                        END AS concat_null_yields_null,
                                        CASE x.transaction_isolation_level
                                            WHEN 0 THEN ''Unspecified''
                                            WHEN 1 THEN ''ReadUncomitted''
                                            WHEN 2 THEN ''ReadCommitted''
                                            WHEN 3 THEN ''Repeatable''
                                            WHEN 4 THEN ''Serializable''
                                            WHEN 5 THEN ''Snapshot''
                                        END AS transaction_isolation_level,
                                        x.lock_timeout,
                                        x.deadlock_priority,
                                        x.row_count,
                                        x.command_type,
                                        ' +
                                        CASE
                                            WHEN OBJECT_ID('master.dbo.fn_varbintohexstr') IS NOT NULL THEN
                                                'master.dbo.fn_varbintohexstr(x.sql_handle) AS sql_handle,
                                                master.dbo.fn_varbintohexstr(x.plan_handle) AS plan_handle,'
                                            ELSE
                                                'CONVERT(VARCHAR(256), x.sql_handle, 1) AS sql_handle,
                                                CONVERT(VARCHAR(256), x.plan_handle, 1) AS plan_handle,'
                                        END +
                                        '
                                        x.statement_start_offset,
                                        x.statement_end_offset,
                                        ' +
                                        CASE
                                            WHEN @output_column_list LIKE '%|[program_name|]%' ESCAPE '|' THEN
                                                '(
                                                    SELECT TOP(1)
                                                        CONVERT(uniqueidentifier, CONVERT(XML, '''').value(''xs:hexBinary( substring(sql:column("agent_info.job_id_string"), 0) )'', ''binary(16)'')) AS job_id,
                                                        agent_info.step_id,
                                                        (
                                                            SELECT TOP(1)
                                                                NULL
                                                            FOR XML
                                                                PATH(''job_name''),
                                                                TYPE
                                                        ),
                                                        (
                                                            SELECT TOP(1)
                                                                NULL
                                                            FOR XML
                                                                PATH(''step_name''),
                                                                TYPE
                                                        )
                                                    FROM
                                                    (
                                                        SELECT TOP(1)
                                                            SUBSTRING(x.program_name, CHARINDEX(''0x'', x.program_name) + 2, 32) AS job_id_string,
                                                            SUBSTRING(x.program_name, CHARINDEX('': Step '', x.program_name) + 7, CHARINDEX('')'', x.program_name, CHARINDEX('': Step '', x.program_name)) - (CHARINDEX('': Step '', x.program_name) + 7)) AS step_id
                                                        WHERE
                                                            x.program_name LIKE N''SQLAgent - TSQL JobStep (Job 0x%''
                                                    ) AS agent_info
                                                    FOR XML
                                                        PATH(''agent_job_info''),
                                                        TYPE
                                                ),
                                                '
                                            ELSE ''
                                        END +
                                        CASE
                                            WHEN @get_task_info = 2 THEN
                                                'CONVERT(XML, x.block_info) AS block_info,
                                                '
                                            ELSE
                                                ''
                                        END + '
                                        x.host_process_id,
                                        x.group_id,
                                        x.original_login_name,
                                        ' +
                                        CASE
                                            WHEN OBJECT_ID('master.dbo.fn_varbintohexstr') IS NOT NULL THEN
                                                'master.dbo.fn_varbintohexstr(x.context_info) AS context_info'
                                            ELSE
                                                'CONVERT(VARCHAR(256), x.context_info, 1) AS context_info'
                                        END + '
                                    FOR XML
                                        PATH(''additional_info''),
                                        TYPE
                                ) '
                    ELSE
                        'NULL '
                END +
                    'AS additional_info,
                    ' +
                CASE
                    WHEN
                        @output_column_list LIKE '%|[memory_info|]%' ESCAPE '|'
                        AND @get_memory_info = 1 THEN'
                        (
                            SELECT TOP(@i)
                            (
                                SELECT TOP(@i)   
                                    x.request_time,
                                    x.grant_time,
                                    x.wait_time_ms,
                                    x.requested_memory_kb,   
                                    x.mg_granted_memory_kb AS granted_memory_kb,
                                    x.mg_used_memory_kb AS used_memory_kb,
                                    x.max_used_memory_kb,
                                    x.ideal_memory_kb,   
                                    x.required_memory_kb,
                                    x.queue_id,
                                    x.wait_order,
                                    x.is_next_candidate,
                                    x.dop,
                                    CAST(x.query_cost AS NUMERIC(38, 4)) AS query_cost
                                FOR XML
                                    PATH(''memory_grant''),
                                    TYPE
                            ),
                            (
                                SELECT TOP(@i)
                                    x.timeout_error_count,
                                    x.target_memory_kb,
                                    x.max_target_memory_kb,
                                    x.total_memory_kb,
                                    x.available_memory_kb,
                                    x.rs_granted_memory_kb AS granted_memory_kb,
                                    x.rs_used_memory_kb AS used_memory_kb,
                                    x.grantee_count,
                                    x.waiter_count
                                FOR XML
                                    PATH(''resource_semaphore''),
                                    TYPE
                            ),
                            (
                                SELECT TOP(@i)   
                                    x.wg_name AS name,
                                    x.request_max_memory_grant_percent,
                                    x.request_max_cpu_time_sec,
                                    x.request_memory_grant_timeout_sec,
                                    x.max_dop
                                FOR XML
                                    PATH(''workload_group''),
                                    TYPE
                            ),
                            (
                                SELECT TOP(@i)   
                                    x.rp_name AS name,
                                    x.min_memory_percent,
                                    x.max_memory_percent,
                                    x.min_cpu_percent,
                                    x.max_cpu_percent
                                FOR XML
                                    PATH(''resource_pool''),
                                    TYPE
                            )
                            WHERE
                                x.request_time IS NOT NULL
                            FOR XML
                                PATH(''memory_info''),
                                TYPE
                        )               
                    '
                    ELSE
                        'NULL '
                END + 'AS memory_info,
                x.start_time,
                '
                +
                CASE
                    WHEN
                        @output_column_list LIKE '%|[login_time|]%' ESCAPE '|'
                        AND @recursion = 1
                            THEN
                                'x.login_time '
                    ELSE
                        'NULL '
                END +
                    'AS login_time,
                x.last_request_start_time
            FROM
            (
                SELECT TOP(@i)
                    y.*,
                    CASE
                        WHEN DATEDIFF(hour, y.start_time, GETDATE()) > 576 THEN
                            DATEDIFF(second, GETDATE(), y.start_time)
                        ELSE DATEDIFF(ms, y.start_time, GETDATE())
                    END AS elapsed_time,
                    COALESCE(tempdb_info.tempdb_allocations, 0) AS tempdb_allocations,
                    COALESCE
                    (
                        CASE
                            WHEN tempdb_info.tempdb_current < 0 THEN 0
                            ELSE tempdb_info.tempdb_current
                        END,
                        0
                    ) AS tempdb_current,
                    ' +
                    CASE
                        WHEN
                            (
                                @get_task_info <> 0
                                OR @find_block_leaders = 1
                            ) THEN
                                'N''('' + CONVERT(NVARCHAR, y.wait_duration_ms) + N''ms)'' +
                                    y.wait_type +
                                        CASE
                                            WHEN y.wait_type LIKE N''PAGE%LATCH_%'' THEN
                                                N'':'' +
                                                COALESCE(DB_NAME(CONVERT(INT, LEFT(y.resource_description, CHARINDEX(N'':'', y.resource_description) - 1))), N''(null)'') +
                                                N'':'' +
                                                SUBSTRING(y.resource_description, CHARINDEX(N'':'', y.resource_description) + 1, LEN(y.resource_description) - CHARINDEX(N'':'', REVERSE(y.resource_description)) - CHARINDEX(N'':'', y.resource_description)) +
                                                N''('' +
                                                    CASE
                                                        WHEN
                                                            CONVERT(INT, RIGHT(y.resource_description, CHARINDEX(N'':'', REVERSE(y.resource_description)) - 1)) = 1 OR
                                                            CONVERT(INT, RIGHT(y.resource_description, CHARINDEX(N'':'', REVERSE(y.resource_description)) - 1)) % 8088 = 0
                                                                THEN
                                                                    N''PFS''
                                                        WHEN
                                                            CONVERT(INT, RIGHT(y.resource_description, CHARINDEX(N'':'', REVERSE(y.resource_description)) - 1)) = 2 OR
                                                            CONVERT(INT, RIGHT(y.resource_description, CHARINDEX(N'':'', REVERSE(y.resource_description)) - 1)) % 511232 = 0
                                                                THEN
                                                                    N''GAM''
                                                        WHEN
                                                            CONVERT(INT, RIGHT(y.resource_description, CHARINDEX(N'':'', REVERSE(y.resource_description)) - 1)) = 3 OR
                                                            (CONVERT(INT, RIGHT(y.resource_description, CHARINDEX(N'':'', REVERSE(y.resource_description)) - 1)) - 1) % 511232 = 0
                                                                THEN
                                                                    N''SGAM''
                                                        WHEN
                                                            CONVERT(INT, RIGHT(y.resource_description, CHARINDEX(N'':'', REVERSE(y.resource_description)) - 1)) = 6 OR
                                                            (CONVERT(INT, RIGHT(y.resource_description, CHARINDEX(N'':'', REVERSE(y.resource_description)) - 1)) - 6) % 511232 = 0
                                                                THEN
                                                                    N''DCM''
                                                        WHEN
                                                            CONVERT(INT, RIGHT(y.resource_description, CHARINDEX(N'':'', REVERSE(y.resource_description)) - 1)) = 7 OR
                                                            (CONVERT(INT, RIGHT(y.resource_description, CHARINDEX(N'':'', REVERSE(y.resource_description)) - 1)) - 7) % 511232 = 0
                                                                THEN
                                                                    N''BCM''
                                                        ELSE
                                                            N''*''
                                                    END +
                                                N'')''
                                            WHEN y.wait_type IN (N''CXPACKET'', N''CXCONSUMER'', N''CXSYNC_PORT'', N''CXSYNC_CONSUMER'') THEN
                                                N'':'' +
                                                    SUBSTRING
                                                    (
                                                        y.resource_description,
                                                        CHARINDEX(N''nodeId'', y.resource_description) + 7,
                                                        CASE
                                                            WHEN CHARINDEX(N'' '', y.resource_description, CHARINDEX(N''nodeId'', y.resource_description)) > 0
                                                            THEN CHARINDEX(N'' '', y.resource_description, CHARINDEX(N''nodeId'', y.resource_description) + 7) - 7 - CHARINDEX(N''nodeId'', y.resource_description)
                                                            ELSE 4
                                                        END
                                                    )
                                            WHEN y.wait_type LIKE N''LATCH[_]%'' THEN
                                                N'' ['' + LEFT(y.resource_description, COALESCE(NULLIF(CHARINDEX(N'' '', y.resource_description), 0), LEN(y.resource_description) + 1) - 1) + N'']''
                                            WHEN
                                                y.wait_type = N''OLEDB''
                                                AND y.resource_description LIKE N''%(SPID=%)'' THEN
                                                    N''['' + LEFT(y.resource_description, CHARINDEX(N''(SPID='', y.resource_description) - 2) +
                                                        N'':'' + SUBSTRING(y.resource_description, CHARINDEX(N''(SPID='', y.resource_description) + 6, CHARINDEX(N'')'', y.resource_description, (CHARINDEX(N''(SPID='', y.resource_description) + 6)) - (CHARINDEX(N''(SPID='', y.resource_description) + 6)) + '']''
                                            ELSE
                                                N''''
                                        END COLLATE Latin1_General_Bin2 AS sys_wait_info,
                                        '
                            ELSE
                                ''
                        END +
                        CASE
                            WHEN @get_task_info = 2 THEN
                                'tasks.physical_io,
                                tasks.context_switches,
                                tasks.tasks,
                                tasks.block_info,
                                tasks.wait_info AS task_wait_info,
                                tasks.thread_CPU_snapshot,
                                '
                            ELSE
                                ''
                    END +
                    CASE
                        WHEN NOT (@get_avg_time = 1 AND @recursion = 1) THEN
                            'CONVERT(INT, NULL) '
                        ELSE
                            'qs.total_elapsed_time / qs.execution_count '
                    END +
                        'AS avg_elapsed_time
                FROM
                (
                    SELECT TOP(@i)
                        sp.session_id,
                        sp.request_id,
                        COALESCE(r.logical_reads, s.logical_reads) AS reads,
                        COALESCE(r.reads, s.reads) AS physical_reads,
                        COALESCE(r.writes, s.writes) AS writes,
                        COALESCE(r.CPU_time, s.CPU_time) AS CPU,
                        ' +
                        CASE
                            WHEN @get_memory_info = 1 THEN
                                'sp.memory_usage AS used_memory,
                                mg.used_memory_kb AS mg_used_memory_kb,
                                mg.max_used_memory_kb,
                                mg.request_time,
                                mg.grant_time,
                                mg.wait_time_ms,
                                mg.requested_memory_kb,
                                mg.granted_memory_kb AS mg_granted_memory_kb,
                                mg.required_memory_kb,
                                mg.ideal_memory_kb,
                                mg.dop AS dop,
                                mg.query_cost AS query_cost,
                                mg.queue_id AS queue_id,
                                mg.wait_order AS wait_order,
                                mg.is_next_candidate,
                                rs.target_memory_kb,
                                rs.max_target_memory_kb,
                                rs.total_memory_kb,
                                rs.available_memory_kb,
                                rs.granted_memory_kb AS rs_granted_memory_kb,
                                rs.used_memory_kb AS rs_used_memory_kb,
                                rs.grantee_count,
                                rs.waiter_count,
                                rs.timeout_error_count,
                                wg.name AS wg_name,
                                wg.request_max_memory_grant_percent,
                                wg.request_max_cpu_time_sec,
                                wg.request_memory_grant_timeout_sec,
                                wg.max_dop,
                                rp.name AS rp_name,
                                rp.min_memory_percent,
                                rp.max_memory_percent,
                                rp.min_cpu_percent,
                                rp.max_cpu_percent,
                                '
                            ELSE
                                'sp.memory_usage + COALESCE(r.granted_query_memory, 0) AS used_memory,
                                '
                        END +
                        'LOWER(sp.status) AS status,
                        COALESCE(r.sql_handle, sp.sql_handle) AS sql_handle,
                        COALESCE(r.statement_start_offset, sp.statement_start_offset) AS statement_start_offset,
                        COALESCE(r.statement_end_offset, sp.statement_end_offset) AS statement_end_offset,
                        ' +
                        CASE
                            WHEN
                            (
                                @get_task_info <> 0
                                OR @find_block_leaders = 1
                            ) THEN
                                'sp.wait_type COLLATE Latin1_General_Bin2 AS wait_type,
                                sp.wait_resource COLLATE Latin1_General_Bin2 AS resource_description,
                                sp.wait_time AS wait_duration_ms,
                                '
                            ELSE
                                ''
                        END +
                        'NULLIF(sp.blocked, 0) AS blocking_session_id,
                        r.plan_handle,
                        NULLIF(r.percent_complete, 0) AS percent_complete,
                        sp.host_name,
                        sp.login_name,
                        sp.program_name,
                        s.host_process_id,
                        COALESCE(r.text_size, s.text_size) AS text_size,
                        COALESCE(r.language, s.language) AS language,
                        COALESCE(r.date_format, s.date_format) AS date_format,
                        COALESCE(r.date_first, s.date_first) AS date_first,
                        COALESCE(r.quoted_identifier, s.quoted_identifier) AS quoted_identifier,
                        COALESCE(r.arithabort, s.arithabort) AS arithabort,
                        COALESCE(r.ansi_null_dflt_on, s.ansi_null_dflt_on) AS ansi_null_dflt_on,
                        COALESCE(r.ansi_defaults, s.ansi_defaults) AS ansi_defaults,
                        COALESCE(r.ansi_warnings, s.ansi_warnings) AS ansi_warnings,
                        COALESCE(r.ansi_padding, s.ansi_padding) AS ansi_padding,
                        COALESCE(r.ansi_nulls, s.ansi_nulls) AS ansi_nulls,
                        COALESCE(r.concat_null_yields_null, s.concat_null_yields_null) AS concat_null_yields_null,
                        COALESCE(r.transaction_isolation_level, s.transaction_isolation_level) AS transaction_isolation_level,
                        COALESCE(r.lock_timeout, s.lock_timeout) AS lock_timeout,
                        COALESCE(r.deadlock_priority, s.deadlock_priority) AS deadlock_priority,
                        COALESCE(r.row_count, s.row_count) AS row_count,
                        COALESCE(r.command, sp.cmd) AS command_type,
                        NULLIF(COALESCE(r.context_info, s.context_info), 0x) AS context_info,
                        COALESCE
                        (
                            CASE
                                WHEN
                                (
                                    s.is_user_process = 0
                                    AND r.total_elapsed_time >= 0
                                ) THEN
                                    DATEADD
                                    (
                                        ms,
                                        1000 * (DATEPART(ms, DATEADD(second, -(r.total_elapsed_time / 1000), GETDATE())) / 500) - DATEPART(ms, DATEADD(second, -(r.total_elapsed_time / 1000), GETDATE())),
                                        DATEADD(second, -(r.total_elapsed_time / 1000), GETDATE())
                                    )
                            END,
                            NULLIF(COALESCE(r.start_time, sp.last_request_end_time), CONVERT(DATETIME, ''19000101'', 112)),
                            sp.login_time
                        ) AS start_time,
                        sp.login_time,
                        CASE
                            WHEN s.is_user_process = 1 THEN
                                s.last_request_start_time
                            ELSE
                                COALESCE
                                (
                                    DATEADD
                                    (
                                        ms,
                                        1000 * (DATEPART(ms, DATEADD(second, -(r.total_elapsed_time / 1000), GETDATE())) / 500) - DATEPART(ms, DATEADD(second, -(r.total_elapsed_time / 1000), GETDATE())),
                                        DATEADD(second, -(r.total_elapsed_time / 1000), GETDATE())
                                    ),
                                    s.last_request_start_time
                                )
                        END AS last_request_start_time,
                        r.transaction_id,
                        sp.database_id,
                        sp.open_tran_count,
                        ' +
                        CASE
                            WHEN EXISTS
                            (
                                SELECT
                                    *
                                FROM sys.all_columns AS ac
                                WHERE
                                    ac.object_id = OBJECT_ID('sys.dm_exec_sessions')
                                    AND ac.name = 'group_id'
                            )
                                THEN 's.group_id,'
                            ELSE 'CONVERT(INT, NULL) AS group_id,'
                        END + '
                        s.original_login_name
                    FROM ' +
                    CASE
                        WHEN @get_memory_info = 1 THEN
                            '(
                                SELECT TOP(@i)
                                    rp0.*
                                FROM sys.resource_governor_resource_pools AS rp0
                            ) AS rp
                            RIGHT OUTER HASH JOIN
                            (
                                (
                                    SELECT TOP(@i)
                                        wg0.*
                                    FROM sys.resource_governor_workload_groups AS wg0
                                ) AS wg
                                RIGHT OUTER HASH JOIN
                                (
                                    (
                                        SELECT TOP(@i)
                                            rs0.*
                                        FROM sys.dm_exec_query_resource_semaphores AS rs0
                                    ) AS rs
                                    RIGHT OUTER HASH JOIN
                                    (
                                        ' + @core_session_join +
                                        'LEFT OUTER LOOP JOIN sys.dm_exec_query_memory_grants AS mg ON
                                            sp.session_id = mg.session_id
                                            AND sp.request_id = mg.request_id
                                    ) ON
                                        rs.resource_semaphore_id = mg.resource_semaphore_id
                                        AND rs.pool_id = mg.pool_id
                                ) ON
                                    wg.group_id = s.group_id
                            ) ON
                                rp.pool_id = wg.pool_id '
                        ELSE @core_session_join
                    END + '
                ) AS y
                ' +
                CASE
                    WHEN @get_task_info = 2 THEN
                        CONVERT(VARCHAR(MAX), '') +
                        'LEFT OUTER HASH JOIN
                        (
                            SELECT TOP(@i)
                                task_nodes.task_node.value(''(session_id/text())[1]'', ''SMALLINT'') AS session_id,
                                task_nodes.task_node.value(''(request_id/text())[1]'', ''INT'') AS request_id,
                                task_nodes.task_node.value(''(physical_io/text())[1]'', ''BIGINT'') AS physical_io,
                                task_nodes.task_node.value(''(context_switches/text())[1]'', ''BIGINT'') AS context_switches,
                                task_nodes.task_node.value(''(tasks/text())[1]'', ''INT'') AS tasks,
                                task_nodes.task_node.value(''(block_info/text())[1]'', ''NVARCHAR(4000)'') AS block_info,
                                task_nodes.task_node.value(''(waits/text())[1]'', ''NVARCHAR(4000)'') AS wait_info,
                                task_nodes.task_node.value(''(thread_CPU_snapshot/text())[1]'', ''BIGINT'') AS thread_CPU_snapshot
                            FROM
                            (
                                SELECT TOP(@i)
                                    CONVERT
                                    (
                                        XML,
                                        REPLACE
                                        (
                                            CONVERT(NVARCHAR(MAX), tasks_raw.task_xml_raw) COLLATE Latin1_General_Bin2,
                                            N''</waits></tasks><tasks><waits>'',
                                            N'', ''
                                        )
                                    ) AS task_xml
                                FROM
                                (
                                    SELECT TOP(@i)
                                        CASE waits.r
                                            WHEN 1 THEN
                                                waits.session_id
                                            ELSE
                                                NULL
                                        END AS [session_id],
                                        CASE waits.r
                                            WHEN 1 THEN
                                                waits.request_id
                                            ELSE
                                                NULL
                                        END AS [request_id],
                                        CASE waits.r
                                            WHEN 1 THEN
                                                waits.physical_io
                                            ELSE
                                                NULL
                                        END AS [physical_io],
                                        CASE waits.r
                                            WHEN 1 THEN
                                                waits.context_switches
                                            ELSE
                                                NULL
                                        END AS [context_switches],
                                        CASE waits.r
                                            WHEN 1 THEN
                                                waits.thread_CPU_snapshot
                                            ELSE
                                                NULL
                                        END AS [thread_CPU_snapshot],
                                        CASE waits.r
                                            WHEN 1 THEN
                                                waits.tasks
                                            ELSE
                                                NULL
                                        END AS [tasks],
                                        CASE waits.r
                                            WHEN 1 THEN
                                                waits.block_info
                                            ELSE
                                                NULL
                                        END AS [block_info],
                                        REPLACE
                                        (
                                            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                                CONVERT
                                                (
                                                    NVARCHAR(MAX),
                                                    N''('' +
                                                        CONVERT(NVARCHAR, num_waits) + N''x: '' +
                                                        CASE num_waits
                                                            WHEN 1 THEN
                                                                CONVERT(NVARCHAR, min_wait_time) + N''ms''
                                                            WHEN 2 THEN
                                                                CASE
                                                                    WHEN min_wait_time <> max_wait_time THEN
                                                                        CONVERT(NVARCHAR, min_wait_time) + N''/'' + CONVERT(NVARCHAR, max_wait_time) + N''ms''
                                                                    ELSE
                                                                        CONVERT(NVARCHAR, max_wait_time) + N''ms''
                                                                END
                                                            ELSE
                                                                CASE
                                                                    WHEN min_wait_time <> max_wait_time THEN
                                                                        CONVERT(NVARCHAR, min_wait_time) + N''/'' + CONVERT(NVARCHAR, avg_wait_time) + N''/'' + CONVERT(NVARCHAR, max_wait_time) + N''ms''
                                                                    ELSE
                                                                        CONVERT(NVARCHAR, max_wait_time) + N''ms''
                                                                END
                                                        END +
                                                    N'')'' + wait_type COLLATE Latin1_General_Bin2
                                                ),
                                                NCHAR(31),N''?''),NCHAR(30),N''?''),NCHAR(29),N''?''),NCHAR(28),N''?''),NCHAR(27),N''?''),NCHAR(26),N''?''),NCHAR(25),N''?''),NCHAR(24),N''?''),NCHAR(23),N''?''),NCHAR(22),N''?''),
                                                NCHAR(21),N''?''),NCHAR(20),N''?''),NCHAR(19),N''?''),NCHAR(18),N''?''),NCHAR(17),N''?''),NCHAR(16),N''?''),NCHAR(15),N''?''),NCHAR(14),N''?''),NCHAR(12),N''?''),
                                                NCHAR(11),N''?''),NCHAR(8),N''?''),NCHAR(7),N''?''),NCHAR(6),N''?''),NCHAR(5),N''?''),NCHAR(4),N''?''),NCHAR(3),N''?''),NCHAR(2),N''?''),NCHAR(1),N''?''),
                                            NCHAR(0),
                                            N''''
                                        ) AS [waits]
                                    FROM
                                    (
                                        SELECT TOP(@i)
                                            w1.*,
                                            ROW_NUMBER() OVER
                                            (
                                                PARTITION BY
                                                    w1.session_id,
                                                    w1.request_id
                                                ORDER BY
                                                    w1.block_info DESC,
                                                    w1.num_waits DESC,
                                                    w1.wait_type
                                            ) AS r
                                        FROM
                                        (
                                            SELECT TOP(@i)
                                                task_info.session_id,
                                                task_info.request_id,
                                                task_info.physical_io,
                                                task_info.context_switches,
                                                task_info.thread_CPU_snapshot,
                                                task_info.num_tasks AS tasks,
                                                CASE
                                                    WHEN task_info.runnable_time IS NOT NULL THEN
                                                        ''RUNNABLE''
                                                    ELSE
                                                        wt2.wait_type
                                                END AS wait_type,
                                                NULLIF(COUNT(COALESCE(task_info.runnable_time, wt2.waiting_task_address)), 0) AS num_waits,
                                                MIN(COALESCE(task_info.runnable_time, wt2.wait_duration_ms)) AS min_wait_time,
                                                AVG(COALESCE(task_info.runnable_time, wt2.wait_duration_ms)) AS avg_wait_time,
                                                MAX(COALESCE(task_info.runnable_time, wt2.wait_duration_ms)) AS max_wait_time,
                                                MAX(wt2.block_info) AS block_info
                                            FROM
                                            (
                                                SELECT TOP(@i)
                                                    t.session_id,
                                                    t.request_id,
                                                    SUM(CONVERT(BIGINT, t.pending_io_count)) OVER (PARTITION BY t.session_id, t.request_id) AS physical_io,
                                                    SUM(CONVERT(BIGINT, t.context_switches_count)) OVER (PARTITION BY t.session_id, t.request_id) AS context_switches,
                                                    ' +
                                                    CASE
                                                        WHEN
                                                            @output_column_list LIKE '%|[CPU_delta|]%' ESCAPE '|'
                                                            AND @sys_info = 1
                                                            THEN
                                                                'SUM(tr.usermode_time + tr.kernel_time) OVER (PARTITION BY t.session_id, t.request_id) '
                                                        ELSE
                                                            'CONVERT(BIGINT, NULL) '
                                                    END +
                                                        ' AS thread_CPU_snapshot,
                                                    COUNT(*) OVER (PARTITION BY t.session_id, t.request_id) AS num_tasks,
                                                    t.task_address,
                                                    t.task_state,
                                                    CASE
                                                        WHEN
                                                            t.task_state = ''RUNNABLE''
                                                            AND w.runnable_time > 0 THEN
                                                                w.runnable_time
                                                        ELSE
                                                            NULL
                                                    END AS runnable_time
                                                FROM sys.dm_os_tasks AS t
                                                CROSS APPLY
                                                (
                                                    SELECT TOP(1)
                                                        sp2.session_id
                                                    FROM @sessions AS sp2
                                                    WHERE
                                                        sp2.session_id = t.session_id
                                                        AND sp2.request_id = t.request_id
                                                        AND sp2.status <> ''sleeping''
                                                ) AS sp20
                                                LEFT OUTER HASH JOIN
                                                (
                                                ' +
                                                    CASE
                                                        WHEN @sys_info = 1 THEN
                                                            'SELECT TOP(@i)
                                                                (
                                                                    SELECT TOP(@i)
                                                                        ms_ticks
                                                                    FROM sys.dm_os_sys_info
                                                                ) -
                                                                    w0.wait_resumed_ms_ticks AS runnable_time,
                                                                w0.worker_address,
                                                                w0.thread_address,
                                                                w0.task_bound_ms_ticks
                                                            FROM sys.dm_os_workers AS w0
                                                            WHERE
                                                                w0.state = ''RUNNABLE''
                                                                OR @first_collection_ms_ticks >= w0.task_bound_ms_ticks'
                                                        ELSE
                                                            'SELECT
                                                                CONVERT(BIGINT, NULL) AS runnable_time,
                                                                CONVERT(VARBINARY(8), NULL) AS worker_address,
                                                                CONVERT(VARBINARY(8), NULL) AS thread_address,
                                                                CONVERT(BIGINT, NULL) AS task_bound_ms_ticks
                                                            WHERE
                                                                1 = 0'
                                                        END +
                                                '
                                                ) AS w ON
                                                    w.worker_address = t.worker_address
                                                ' +
                                                CASE
                                                    WHEN
                                                        @output_column_list LIKE '%|[CPU_delta|]%' ESCAPE '|'
                                                        AND @sys_info = 1
                                                        THEN
                                                            'LEFT OUTER HASH JOIN sys.dm_os_threads AS tr ON
                                                                tr.thread_address = w.thread_address
                                                                AND @first_collection_ms_ticks >= w.task_bound_ms_ticks
                                                            '
                                                    ELSE
                                                        ''
                                                END +
                                            ') AS task_info
                                            LEFT OUTER HASH JOIN
                                            (
                                                SELECT TOP(@i)
                                                    wt1.wait_type,
                                                    wt1.waiting_task_address,
                                                    MAX(wt1.wait_duration_ms) AS wait_duration_ms,
                                                    MAX(wt1.block_info) AS block_info
                                                FROM
                                                (
                                                    SELECT DISTINCT TOP(@i)
                                                        wt.wait_type +
                                                            CASE
                                                                WHEN wt.wait_type LIKE N''PAGE%LATCH_%'' THEN
                                                                    '':'' +
                                                                    COALESCE(DB_NAME(CONVERT(INT, LEFT(wt.resource_description, CHARINDEX(N'':'', wt.resource_description) - 1))), N''(null)'') +
                                                                    N'':'' +
                                                                    SUBSTRING(wt.resource_description, CHARINDEX(N'':'', wt.resource_description) + 1, LEN(wt.resource_description) - CHARINDEX(N'':'', REVERSE(wt.resource_description)) - CHARINDEX(N'':'', wt.resource_description)) +
                                                                    N''('' +
                                                                        CASE
                                                                            WHEN
                                                                                CONVERT(INT, RIGHT(wt.resource_description, CHARINDEX(N'':'', REVERSE(wt.resource_description)) - 1)) = 1 OR
                                                                                CONVERT(INT, RIGHT(wt.resource_description, CHARINDEX(N'':'', REVERSE(wt.resource_description)) - 1)) % 8088 = 0
                                                                                    THEN
                                                                                        N''PFS''
                                                                            WHEN
                                                                                CONVERT(INT, RIGHT(wt.resource_description, CHARINDEX(N'':'', REVERSE(wt.resource_description)) - 1)) = 2 OR
                                                                                CONVERT(INT, RIGHT(wt.resource_description, CHARINDEX(N'':'', REVERSE(wt.resource_description)) - 1)) % 511232 = 0
                                                                                    THEN
                                                                                        N''GAM''
                                                                            WHEN
                                                                                CONVERT(INT, RIGHT(wt.resource_description, CHARINDEX(N'':'', REVERSE(wt.resource_description)) - 1)) = 3 OR
                                                                                (CONVERT(INT, RIGHT(wt.resource_description, CHARINDEX(N'':'', REVERSE(wt.resource_description)) - 1)) - 1) % 511232 = 0
                                                                                    THEN
                                                                                        N''SGAM''
                                                                            WHEN
                                                                                CONVERT(INT, RIGHT(wt.resource_description, CHARINDEX(N'':'', REVERSE(wt.resource_description)) - 1)) = 6 OR
                                                                                (CONVERT(INT, RIGHT(wt.resource_description, CHARINDEX(N'':'', REVERSE(wt.resource_description)) - 1)) - 6) % 511232 = 0
                                                                                    THEN
                                                                                        N''DCM''
                                                                            WHEN
                                                                                CONVERT(INT, RIGHT(wt.resource_description, CHARINDEX(N'':'', REVERSE(wt.resource_description)) - 1)) = 7 OR
                                                                                (CONVERT(INT, RIGHT(wt.resource_description, CHARINDEX(N'':'', REVERSE(wt.resource_description)) - 1)) - 7) % 511232 = 0
                                                                                    THEN
                                                                                        N''BCM''
                                                                            ELSE
                                                                                N''*''
                                                                        END +
                                                                    N'')''
                                                                WHEN wt.wait_type IN (N''CXPACKET'', N''CXCONSUMER'', N''CXSYNC_PORT'', N''CXSYNC_CONSUMER'') THEN
                                                                    N'':'' +
                                                                        SUBSTRING
                                                                        (
                                                                            wt.resource_description,
                                                                            CHARINDEX(N''nodeId'', wt.resource_description) + 7,
                                                                            CASE
                                                                                WHEN CHARINDEX(N'' '', wt.resource_description, CHARINDEX(N''nodeId'', wt.resource_description)) > 0
                                                                                 THEN CHARINDEX(N'' '', wt.resource_description, CHARINDEX(N''nodeId'', wt.resource_description) + 7) - 7 - CHARINDEX(N''nodeId'', wt.resource_description)
                                                                                ELSE 4
                                                                            END
                                                                        )
                                                                WHEN wt.wait_type LIKE N''LATCH[_]%'' THEN
                                                                    N'' ['' + LEFT(wt.resource_description, COALESCE(NULLIF(CHARINDEX(N'' '', wt.resource_description), 0), LEN(wt.resource_description) + 1) - 1) + N'']''
                                                                ELSE
                                                                    N''''
                                                            END COLLATE Latin1_General_Bin2 AS wait_type,
                                                        CASE
                                                            WHEN
                                                            (
                                                                wt.blocking_session_id IS NOT NULL
                                                                AND wt.wait_type LIKE N''LCK[_]%''
                                                            ) THEN
                                                                (
                                                                    SELECT TOP(@i)
                                                                        x.lock_type,
                                                                        REPLACE
                                                                        (
                                                                            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                                                            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                                                            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                                                                DB_NAME
                                                                                (
                                                                                    CONVERT
                                                                                    (
                                                                                        INT,
                                                                                        SUBSTRING(wt.resource_description, NULLIF(CHARINDEX(N''dbid='', wt.resource_description), 0) + 5, COALESCE(NULLIF(CHARINDEX(N'' '', wt.resource_description, CHARINDEX(N''dbid='', wt.resource_description) + 5), 0), LEN(wt.resource_description) + 1) - CHARINDEX(N''dbid='', wt.resource_description) - 5)
                                                                                    )
                                                                                ),
                                                                                NCHAR(31),N''?''),NCHAR(30),N''?''),NCHAR(29),N''?''),NCHAR(28),N''?''),NCHAR(27),N''?''),NCHAR(26),N''?''),NCHAR(25),N''?''),NCHAR(24),N''?''),NCHAR(23),N''?''),NCHAR(22),N''?''),
                                                                                NCHAR(21),N''?''),NCHAR(20),N''?''),NCHAR(19),N''?''),NCHAR(18),N''?''),NCHAR(17),N''?''),NCHAR(16),N''?''),NCHAR(15),N''?''),NCHAR(14),N''?''),NCHAR(12),N''?''),
                                                                                NCHAR(11),N''?''),NCHAR(8),N''?''),NCHAR(7),N''?''),NCHAR(6),N''?''),NCHAR(5),N''?''),NCHAR(4),N''?''),NCHAR(3),N''?''),NCHAR(2),N''?''),NCHAR(1),N''?''),
                                                                            NCHAR(0),
                                                                            N''''
                                                                        ) AS database_name,
                                                                        CASE x.lock_type
                                                                            WHEN N''objectlock'' THEN
                                                                                SUBSTRING(wt.resource_description, NULLIF(CHARINDEX(N''objid='', wt.resource_description), 0) + 6, COALESCE(NULLIF(CHARINDEX(N'' '', wt.resource_description, CHARINDEX(N''objid='', wt.resource_description) + 6), 0), LEN(wt.resource_description) + 1) - CHARINDEX(N''objid='', wt.resource_description) - 6)
                                                                            ELSE
                                                                                NULL
                                                                        END AS object_id,
                                                                        CASE x.lock_type
                                                                            WHEN N''filelock'' THEN
                                                                                SUBSTRING(wt.resource_description, NULLIF(CHARINDEX(N''fileid='', wt.resource_description), 0) + 7, COALESCE(NULLIF(CHARINDEX(N'' '', wt.resource_description, CHARINDEX(N''fileid='', wt.resource_description) + 7), 0), LEN(wt.resource_description) + 1) - CHARINDEX(N''fileid='', wt.resource_description) - 7)
                                                                            ELSE
                                                                                NULL
                                                                        END AS file_id,
                                                                        CASE
                                                                            WHEN x.lock_type in (N''pagelock'', N''extentlock'', N''ridlock'') THEN
                                                                                SUBSTRING(wt.resource_description, NULLIF(CHARINDEX(N''associatedObjectId='', wt.resource_description), 0) + 19, COALESCE(NULLIF(CHARINDEX(N'' '', wt.resource_description, CHARINDEX(N''associatedObjectId='', wt.resource_description) + 19), 0), LEN(wt.resource_description) + 1) - CHARINDEX(N''associatedObjectId='', wt.resource_description) - 19)
                                                                            WHEN x.lock_type in (N''keylock'', N''hobtlock'', N''allocunitlock'') THEN
                                                                                SUBSTRING(wt.resource_description, NULLIF(CHARINDEX(N''hobtid='', wt.resource_description), 0) + 7, COALESCE(NULLIF(CHARINDEX(N'' '', wt.resource_description, CHARINDEX(N''hobtid='', wt.resource_description) + 7), 0), LEN(wt.resource_description) + 1) - CHARINDEX(N''hobtid='', wt.resource_description) - 7)
                                                                            ELSE
                                                                                NULL
                                                                        END AS hobt_id,
                                                                        CASE x.lock_type
                                                                            WHEN N''applicationlock'' THEN
                                                                                SUBSTRING(wt.resource_description, NULLIF(CHARINDEX(N''hash='', wt.resource_description), 0) + 5, COALESCE(NULLIF(CHARINDEX(N'' '', wt.resource_description, CHARINDEX(N''hash='', wt.resource_description) + 5), 0), LEN(wt.resource_description) + 1) - CHARINDEX(N''hash='', wt.resource_description) - 5)
                                                                            ELSE
                                                                                NULL
                                                                        END AS applock_hash,
                                                                        CASE x.lock_type
                                                                            WHEN N''metadatalock'' THEN
                                                                                SUBSTRING(wt.resource_description, NULLIF(CHARINDEX(N''subresource='', wt.resource_description), 0) + 12, COALESCE(NULLIF(CHARINDEX(N'' '', wt.resource_description, CHARINDEX(N''subresource='', wt.resource_description) + 12), 0), LEN(wt.resource_description) + 1) - CHARINDEX(N''subresource='', wt.resource_description) - 12)
                                                                            ELSE
                                                                                NULL
                                                                        END AS metadata_resource,
                                                                        CASE x.lock_type
                                                                            WHEN N''metadatalock'' THEN
                                                                                SUBSTRING(wt.resource_description, NULLIF(CHARINDEX(N''classid='', wt.resource_description), 0) + 8, COALESCE(NULLIF(CHARINDEX(N'' dbid='', wt.resource_description) - CHARINDEX(N''classid='', wt.resource_description), 0), LEN(wt.resource_description) + 1) - 8)
                                                                            ELSE
                                                                                NULL
                                                                        END AS metadata_class_id
                                                                    FROM
                                                                    (
                                                                        SELECT TOP(1)
                                                                            LEFT(wt.resource_description, CHARINDEX(N'' '', wt.resource_description) - 1) COLLATE Latin1_General_Bin2 AS lock_type
                                                                    ) AS x
                                                                    FOR XML
                                                                        PATH('''')
                                                                )
                                                            ELSE NULL
                                                        END AS block_info,
                                                        wt.wait_duration_ms,
                                                        wt.waiting_task_address
                                                    FROM
                                                    (
                                                        SELECT TOP(@i)
                                                            wt0.wait_type COLLATE Latin1_General_Bin2 AS wait_type,
                                                            LEFT
                                                            (
                                                                p.resource_description,
                                                                ISNULL(NULLIF(CHARINDEX('' (LATCH '', p.resource_description) - 1, -1), 256)
                                                            ) AS resource_description,
                                                            wt0.wait_duration_ms,
                                                            wt0.waiting_task_address,
                                                            CASE
                                                                WHEN wt0.blocking_session_id = p.blocked THEN
                                                                    wt0.blocking_session_id
                                                                ELSE
                                                                    NULL
                                                            END AS blocking_session_id
                                                        FROM sys.dm_os_waiting_tasks AS wt0
                                                        CROSS APPLY
                                                        (
                                                            SELECT TOP(1)
                                                                s0.blocked,
                                                                wt0.resource_description COLLATE Latin1_General_Bin2 AS resource_description
                                                            FROM @sessions AS s0
                                                            WHERE
                                                                s0.session_id = wt0.session_id
                                                                AND COALESCE(s0.wait_type, N'''') <> N''OLEDB''
                                                                AND wt0.wait_type <> N''OLEDB''
                                                        ) AS p
                                                    ) AS wt
                                                ) AS wt1
                                                GROUP BY
                                                    wt1.wait_type,
                                                    wt1.waiting_task_address
                                            ) AS wt2 ON
                                                wt2.waiting_task_address = task_info.task_address
                                                AND wt2.wait_duration_ms > 0
                                                AND task_info.runnable_time IS NULL
                                            GROUP BY
                                                task_info.session_id,
                                                task_info.request_id,
                                                task_info.physical_io,
                                                task_info.context_switches,
                                                task_info.thread_CPU_snapshot,
                                                task_info.num_tasks,
                                                CASE
                                                    WHEN task_info.runnable_time IS NOT NULL THEN
                                                        ''RUNNABLE''
                                                    ELSE
                                                        wt2.wait_type
                                                END
                                        ) AS w1
                                    ) AS waits
                                    ORDER BY
                                        waits.session_id,
                                        waits.request_id,
                                        waits.r
                                    FOR XML
                                        PATH(N''tasks''),
                                        TYPE
                                ) AS tasks_raw (task_xml_raw)
                            ) AS tasks_final
                            CROSS APPLY tasks_final.task_xml.nodes(N''/tasks'') AS task_nodes (task_node)
                            WHERE
                                task_nodes.task_node.exist(N''session_id'') = 1
                        ) AS tasks ON
                            tasks.session_id = y.session_id
                            AND tasks.request_id = y.request_id
                        '
                    ELSE
                        ''
                END +
                'LEFT OUTER HASH JOIN
                (
                    SELECT TOP(@i)
                        t_info.session_id,
                        COALESCE(t_info.request_id, -1) AS request_id,
                        SUM(t_info.tempdb_allocations) AS tempdb_allocations,
                        SUM(t_info.tempdb_current) AS tempdb_current
                    FROM
                    (
                        SELECT TOP(@i)
                            tsu.session_id,
                            tsu.request_id,
                            tsu.user_objects_alloc_page_count +
                                tsu.internal_objects_alloc_page_count AS tempdb_allocations,
                            tsu.user_objects_alloc_page_count +
                                tsu.internal_objects_alloc_page_count -
                                tsu.user_objects_dealloc_page_count -
                                tsu.internal_objects_dealloc_page_count AS tempdb_current
                        FROM sys.dm_db_task_space_usage AS tsu
                        CROSS APPLY
                        (
                            SELECT TOP(1)
                                s0.session_id
                            FROM @sessions AS s0
                            WHERE
                                s0.session_id = tsu.session_id
                        ) AS p

                        UNION ALL

                        SELECT TOP(@i)
                            ssu.session_id,
                            NULL AS request_id,
                            ssu.user_objects_alloc_page_count +
                                ssu.internal_objects_alloc_page_count AS tempdb_allocations,
                            ssu.user_objects_alloc_page_count +
                                ssu.internal_objects_alloc_page_count -
                                ssu.user_objects_dealloc_page_count -
                                ssu.internal_objects_dealloc_page_count AS tempdb_current
                        FROM sys.dm_db_session_space_usage AS ssu
                        CROSS APPLY
                        (
                            SELECT TOP(1)
                                s0.session_id
                            FROM @sessions AS s0
                            WHERE
                                s0.session_id = ssu.session_id
                        ) AS p
                    ) AS t_info
                    GROUP BY
                        t_info.session_id,
                        COALESCE(t_info.request_id, -1)
                ) AS tempdb_info ON
                    tempdb_info.session_id = y.session_id
                    AND tempdb_info.request_id =
                        CASE
                            WHEN y.status = N''sleeping'' THEN
                                -1
                            ELSE
                                y.request_id
                        END
                ' +
                CASE
                    WHEN
                        NOT
                        (
                            @get_avg_time = 1
                            AND @recursion = 1
                        ) THEN
                            ''
                    ELSE
                        'LEFT OUTER HASH JOIN
                        (
                            SELECT TOP(@i)
                                *
                            FROM sys.dm_exec_query_stats
                        ) AS qs ON
                            qs.sql_handle = y.sql_handle
                            AND qs.plan_handle = y.plan_handle
                            AND qs.statement_start_offset = y.statement_start_offset
                            AND qs.statement_end_offset = y.statement_end_offset
                        '
                END +
            ') AS x
            OPTION (KEEPFIXED PLAN, OPTIMIZE FOR (@i = 1)); ';

        SET @sql_n = CONVERT(NVARCHAR(MAX), @sql);

        SET @last_collection_start = GETDATE();

        IF
            @recursion = -1
            AND @sys_info = 1
        BEGIN;
            SELECT
                @first_collection_ms_ticks = ms_ticks
            FROM sys.dm_os_sys_info;
        END;

        INSERT #sessions
        (
            recursion,
            session_id,
            request_id,
            session_number,
            elapsed_time,
            avg_elapsed_time,
            physical_io,
            reads,
            physical_reads,
            writes,
            tempdb_allocations,
            tempdb_current,
            CPU,
            thread_CPU_snapshot,
            context_switches,
            used_memory,
            max_used_memory,
            requested_memory,
            granted_memory,
            tasks,
            status,
            wait_info,
            transaction_id,
            open_tran_count,
            sql_handle,
            statement_start_offset,
            statement_end_offset,       
            sql_text,
            plan_handle,
            blocking_session_id,
            percent_complete,
            host_name,
            login_name,
            database_name,
            program_name,
            additional_info,
            memory_info,
            start_time,
            login_time,
            last_request_start_time
        )
        EXEC sp_executesql
            @sql_n,
            N'@recursion SMALLINT, @filter sysname, @not_filter sysname, @first_collection_ms_ticks BIGINT',
            @recursion, @filter, @not_filter, @first_collection_ms_ticks;

        --Collect transaction information?
        IF
            @recursion = 1
            AND
            (
                @output_column_list LIKE '%|[tran_start_time|]%' ESCAPE '|'
                OR @output_column_list LIKE '%|[tran_log_writes|]%' ESCAPE '|'
                OR @output_column_list LIKE '%|[implicit_tran|]%' ESCAPE '|'
            )
        BEGIN;   
            DECLARE @i INT;
            SET @i = 2147483647;

            UPDATE s
            SET
                tran_start_time =
                    CONVERT
                    (
                        DATETIME,
                        LEFT
                        (
                            x.trans_info,
                            NULLIF(CHARINDEX(NCHAR(254) COLLATE Latin1_General_Bin2, x.trans_info) - 1, -1)
                        ),
                        121
                    ),
                tran_log_writes =
                    RIGHT
                    (
                        x.trans_info,
                        LEN(x.trans_info) - CHARINDEX(NCHAR(254) COLLATE Latin1_General_Bin2, x.trans_info)
                    ),
                implicit_tran =
                    CASE
                        WHEN x.implicit_tran = 1 THEN 'ON'
                        ELSE 'OFF'
                    END
            FROM
            (
                SELECT TOP(@i)
                    trans_nodes.trans_node.value('(session_id/text())[1]', 'SMALLINT') AS session_id,
                    COALESCE(trans_nodes.trans_node.value('(request_id/text())[1]', 'INT'), 0) AS request_id,
                    trans_nodes.trans_node.value('(implicit_tran/text())[1]', 'INT') AS implicit_tran,
                    trans_nodes.trans_node.value('(trans_info/text())[1]', 'NVARCHAR(4000)') AS trans_info
                FROM
                (
                    SELECT TOP(@i)
                        CONVERT
                        (
                            XML,
                            REPLACE
                            (
                                CONVERT(NVARCHAR(MAX), trans_raw.trans_xml_raw) COLLATE Latin1_General_Bin2,
                                N'</trans_info></trans><trans><trans_info>', N''
                            )
                        )
                    FROM
                    (
                        SELECT TOP(@i)
                            CASE u_trans.r
                                WHEN 1 THEN u_trans.session_id
                                ELSE NULL
                            END AS [session_id],
                            CASE u_trans.r
                                WHEN 1 THEN u_trans.request_id
                                ELSE NULL
                            END AS [request_id],
                            u_trans.implicit_tran AS [implicit_tran],
                            CONVERT
                            (
                                NVARCHAR(MAX),
                                CASE
                                    WHEN u_trans.database_id IS NOT NULL THEN
                                        CASE u_trans.r
                                            WHEN 1 THEN COALESCE(CONVERT(NVARCHAR, u_trans.transaction_start_time, 121) + NCHAR(254), N'')
                                            ELSE N''
                                        END +
                                            REPLACE
                                            (
                                                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                                    CONVERT(VARCHAR(128), COALESCE(DB_NAME(u_trans.database_id), N'(null)')),
                                                    NCHAR(31),N'?'),NCHAR(30),N'?'),NCHAR(29),N'?'),NCHAR(28),N'?'),NCHAR(27),N'?'),NCHAR(26),N'?'),NCHAR(25),N'?'),NCHAR(24),N'?'),NCHAR(23),N'?'),NCHAR(22),N'?'),
                                                    NCHAR(21),N'?'),NCHAR(20),N'?'),NCHAR(19),N'?'),NCHAR(18),N'?'),NCHAR(17),N'?'),NCHAR(16),N'?'),NCHAR(15),N'?'),NCHAR(14),N'?'),NCHAR(12),N'?'),
                                                    NCHAR(11),N'?'),NCHAR(8),N'?'),NCHAR(7),N'?'),NCHAR(6),N'?'),NCHAR(5),N'?'),NCHAR(4),N'?'),NCHAR(3),N'?'),NCHAR(2),N'?'),NCHAR(1),N'?'),
                                                NCHAR(0),
                                                N'?'
                                            ) +
                                            N': ' +
                                        CONVERT(NVARCHAR, u_trans.log_record_count) + N' (' + CONVERT(NVARCHAR, u_trans.log_kb_used) + N' kB)' +
                                        N','
                                    ELSE
                                        N'N/A,'
                                END COLLATE Latin1_General_Bin2
                            ) AS [trans_info]
                        FROM
                        (
                            SELECT TOP(@i)
                                trans.*,
                                ROW_NUMBER() OVER
                                (
                                    PARTITION BY
                                        trans.session_id,
                                        trans.request_id
                                    ORDER BY
                                        trans.transaction_start_time DESC
                                ) AS r
                            FROM
                            (
                                SELECT TOP(@i)
                                    session_tran_map.session_id,
                                    session_tran_map.request_id,
                                    s_tran.database_id,
                                    COALESCE(SUM(s_tran.database_transaction_log_record_count), 0) AS log_record_count,
                                    COALESCE(SUM(s_tran.database_transaction_log_bytes_used), 0) / 1024 AS log_kb_used,
                                    MIN(s_tran.database_transaction_begin_time) AS transaction_start_time,
                                    MAX
                                    (
                                        CASE
                                            WHEN a_tran.name = 'implicit_transaction' THEN 1
                                            ELSE 0
                                        END
                                    ) AS implicit_tran
                                FROM
                                (
                                    SELECT TOP(@i)
                                        *
                                    FROM sys.dm_tran_active_transactions
                                    WHERE
                                        transaction_begin_time <= @last_collection_start
                                ) AS a_tran
                                INNER HASH JOIN
                                (
                                    SELECT TOP(@i)
                                        *
                                    FROM sys.dm_tran_database_transactions
                                    WHERE
                                        database_id < 32767
                                ) AS s_tran ON
                                    s_tran.transaction_id = a_tran.transaction_id
                                LEFT OUTER HASH JOIN
                                (
                                    SELECT TOP(@i)
                                        *
                                    FROM sys.dm_tran_session_transactions
                                ) AS tst ON
                                    s_tran.transaction_id = tst.transaction_id
                                CROSS APPLY
                                (
                                    SELECT TOP(1)
                                        s3.session_id,
                                        s3.request_id
                                    FROM
                                    (
                                        SELECT TOP(1)
                                            s1.session_id,
                                            s1.request_id
                                        FROM #sessions AS s1
                                        WHERE
                                            s1.transaction_id = s_tran.transaction_id
                                            AND s1.recursion = 1
                                           
                                        UNION ALL
                                   
                                        SELECT TOP(1)
                                            s2.session_id,
                                            s2.request_id
                                        FROM #sessions AS s2
                                        WHERE
                                            s2.session_id = tst.session_id
                                            AND s2.recursion = 1
                                    ) AS s3
                                    ORDER BY
                                        s3.request_id
                                ) AS session_tran_map
                                GROUP BY
                                    session_tran_map.session_id,
                                    session_tran_map.request_id,
                                    s_tran.database_id
                            ) AS trans
                        ) AS u_trans
                        FOR XML
                            PATH('trans'),
                            TYPE
                    ) AS trans_raw (trans_xml_raw)
                ) AS trans_final (trans_xml)
                CROSS APPLY trans_final.trans_xml.nodes('/trans') AS trans_nodes (trans_node)
            ) AS x
            INNER HASH JOIN #sessions AS s ON
                s.session_id = x.session_id
                AND s.request_id = x.request_id
            OPTION (OPTIMIZE FOR (@i = 1));
        END;

        --Variables for text and plan collection
        DECLARE   
            @session_id SMALLINT,
            @request_id INT,
            @sql_handle VARBINARY(64),
            @plan_handle VARBINARY(64),
            @statement_start_offset INT,
            @statement_end_offset INT,
            @start_time DATETIME,
            @database_name sysname;

        IF
            @recursion = 1
            AND @output_column_list LIKE '%|[sql_text|]%' ESCAPE '|'
        BEGIN;
            DECLARE sql_cursor
            CURSOR LOCAL FAST_FORWARD
            FOR
                SELECT
                    session_id,
                    request_id,
                    sql_handle,
                    statement_start_offset,
                    statement_end_offset
                FROM #sessions
                WHERE
                    recursion = 1
                    AND sql_handle IS NOT NULL
            OPTION (KEEPFIXED PLAN);

            OPEN sql_cursor;

            FETCH NEXT FROM sql_cursor
            INTO
                @session_id,
                @request_id,
                @sql_handle,
                @statement_start_offset,
                @statement_end_offset;

            --Wait up to 5 ms for the SQL text, then give up
            SET LOCK_TIMEOUT 5;

            WHILE @@FETCH_STATUS = 0
            BEGIN;
                BEGIN TRY;
                    UPDATE s
                    SET
                        s.sql_text =
                        (
                            SELECT
                                REPLACE
                                (
                                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                        N'--' + NCHAR(13) + NCHAR(10) +
                                        CASE
                                            WHEN @get_full_inner_text = 1 THEN est.text
                                            WHEN LEN(est.text) < (@statement_end_offset / 2) + 1 THEN est.text
                                            WHEN SUBSTRING(est.text, (@statement_start_offset/2), 2) LIKE N'[a-zA-Z0-9][a-zA-Z0-9]' THEN est.text
                                            ELSE
                                                CASE
                                                    WHEN @statement_start_offset > 0 THEN
                                                        SUBSTRING
                                                        (
                                                            est.text,
                                                            ((@statement_start_offset/2) + 1),
                                                            (
                                                                CASE
                                                                    WHEN @statement_end_offset = -1 THEN 2147483647
                                                                    ELSE ((@statement_end_offset - @statement_start_offset)/2) + 1
                                                                END
                                                            )
                                                        )
                                                    ELSE RTRIM(LTRIM(est.text))
                                                END
                                        END +
                                        NCHAR(13) + NCHAR(10) + N'--' COLLATE Latin1_General_Bin2,
                                        NCHAR(31),N'?'),NCHAR(30),N'?'),NCHAR(29),N'?'),NCHAR(28),N'?'),NCHAR(27),N'?'),NCHAR(26),N'?'),NCHAR(25),N'?'),NCHAR(24),N'?'),NCHAR(23),N'?'),NCHAR(22),N'?'),
                                        NCHAR(21),N'?'),NCHAR(20),N'?'),NCHAR(19),N'?'),NCHAR(18),N'?'),NCHAR(17),N'?'),NCHAR(16),N'?'),NCHAR(15),N'?'),NCHAR(14),N'?'),NCHAR(12),N'?'),
                                        NCHAR(11),N'?'),NCHAR(8),N'?'),NCHAR(7),N'?'),NCHAR(6),N'?'),NCHAR(5),N'?'),NCHAR(4),N'?'),NCHAR(3),N'?'),NCHAR(2),N'?'),NCHAR(1),N'?'),
                                    NCHAR(0),
                                    N''
                                ) AS [processing-instruction(query)]
                            FOR XML
                                PATH(''),
                                TYPE
                        ),
                        s.statement_start_offset =
                            CASE
                                WHEN LEN(est.text) < (@statement_end_offset / 2) + 1 THEN 0
                                WHEN SUBSTRING(CONVERT(VARCHAR(MAX), est.text), (@statement_start_offset/2), 2) LIKE '[a-zA-Z0-9][a-zA-Z0-9]' THEN 0
                                ELSE @statement_start_offset
                            END,
                        s.statement_end_offset =
                            CASE
                                WHEN LEN(est.text) < (@statement_end_offset / 2) + 1 THEN -1
                                WHEN SUBSTRING(CONVERT(VARCHAR(MAX), est.text), (@statement_start_offset/2), 2) LIKE '[a-zA-Z0-9][a-zA-Z0-9]' THEN -1
                                ELSE @statement_end_offset
                            END
                    FROM
                        #sessions AS s,
                        (
                            SELECT TOP(1)
                                text
                            FROM
                            (
                                SELECT
                                    text,
                                    0 AS row_num
                                FROM sys.dm_exec_sql_text(@sql_handle)
                               
                                UNION ALL
                               
                                SELECT
                                    NULL,
                                    1 AS row_num
                            ) AS est0
                            ORDER BY
                                row_num
                        ) AS est
                    WHERE
                        s.session_id = @session_id
                        AND s.request_id = @request_id
                        AND s.recursion = 1
                    OPTION (KEEPFIXED PLAN);
                END TRY
                BEGIN CATCH;
                    UPDATE s
                    SET
                        s.sql_text =
                            CASE ERROR_NUMBER()
                                WHEN 1222 THEN '<timeout_exceeded />'
                                ELSE '<error message="' + ERROR_MESSAGE() + '" />'
                            END
                    FROM #sessions AS s
                    WHERE
                        s.session_id = @session_id
                        AND s.request_id = @request_id
                        AND s.recursion = 1
                    OPTION (KEEPFIXED PLAN);
                END CATCH;

                FETCH NEXT FROM sql_cursor
                INTO
                    @session_id,
                    @request_id,
                    @sql_handle,
                    @statement_start_offset,
                    @statement_end_offset;
            END;

            --Return this to the default
            SET LOCK_TIMEOUT -1;

            CLOSE sql_cursor;
            DEALLOCATE sql_cursor;
        END;

        IF
            @get_outer_command = 1
            AND @recursion = 1
            AND @output_column_list LIKE '%|[sql_command|]%' ESCAPE '|'
        BEGIN;
            DECLARE @buffer_results TABLE
            (
                EventType VARCHAR(30),
                Parameters INT,
                EventInfo NVARCHAR(4000),
                start_time DATETIME,
                session_number INT IDENTITY(1,1) NOT NULL PRIMARY KEY
            );

            DECLARE buffer_cursor
            CURSOR LOCAL FAST_FORWARD
            FOR
                SELECT
                    session_id,
                    MAX(start_time) AS start_time
                FROM #sessions
                WHERE
                    recursion = 1
                GROUP BY
                    session_id
                ORDER BY
                    session_id
                OPTION (KEEPFIXED PLAN);

            OPEN buffer_cursor;

            FETCH NEXT FROM buffer_cursor
            INTO
                @session_id,
                @start_time;

            WHILE @@FETCH_STATUS = 0
            BEGIN;
                BEGIN TRY;
                    --In SQL Server 2008, DBCC INPUTBUFFER will throw
                    --an exception if the session no longer exists
                    INSERT @buffer_results
                    (
                        EventType,
                        Parameters,
                        EventInfo
                    )
                    EXEC sp_executesql
                        N'DBCC INPUTBUFFER(@session_id) WITH NO_INFOMSGS;',
                        N'@session_id SMALLINT',
                        @session_id;

                    UPDATE br
                    SET
                        br.start_time = @start_time
                    FROM @buffer_results AS br
                    WHERE
                        br.session_number =
                        (
                            SELECT MAX(br2.session_number)
                            FROM @buffer_results br2
                        );
                END TRY
                BEGIN CATCH
                END CATCH;

                FETCH NEXT FROM buffer_cursor
                INTO
                    @session_id,
                    @start_time;
            END;

            UPDATE s
            SET
                sql_command =
                (
                    SELECT
                        REPLACE
                        (
                            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                CONVERT
                                (
                                    NVARCHAR(MAX),
                                    N'--' + NCHAR(13) + NCHAR(10) + br.EventInfo + NCHAR(13) + NCHAR(10) + N'--' COLLATE Latin1_General_Bin2
                                ),
                                NCHAR(31),N'?'),NCHAR(30),N'?'),NCHAR(29),N'?'),NCHAR(28),N'?'),NCHAR(27),N'?'),NCHAR(26),N'?'),NCHAR(25),N'?'),NCHAR(24),N'?'),NCHAR(23),N'?'),NCHAR(22),N'?'),
                                NCHAR(21),N'?'),NCHAR(20),N'?'),NCHAR(19),N'?'),NCHAR(18),N'?'),NCHAR(17),N'?'),NCHAR(16),N'?'),NCHAR(15),N'?'),NCHAR(14),N'?'),NCHAR(12),N'?'),
                                NCHAR(11),N'?'),NCHAR(8),N'?'),NCHAR(7),N'?'),NCHAR(6),N'?'),NCHAR(5),N'?'),NCHAR(4),N'?'),NCHAR(3),N'?'),NCHAR(2),N'?'),NCHAR(1),N'?'),
                            NCHAR(0),
                            N''
                        ) AS [processing-instruction(query)]
                    FROM @buffer_results AS br
                    WHERE
                        br.session_number = s.session_number
                        AND br.start_time = s.start_time
                        AND
                        (
                            (
                                s.start_time = s.last_request_start_time
                                AND EXISTS
                                (
                                    SELECT *
                                    FROM sys.dm_exec_requests r2
                                    WHERE
                                        r2.session_id = s.session_id
                                        AND r2.request_id = s.request_id
                                        AND r2.start_time = s.start_time
                                )
                            )
                            OR
                            (
                                s.request_id = 0
                                AND EXISTS
                                (
                                    SELECT *
                                    FROM sys.dm_exec_sessions s2
                                    WHERE
                                        s2.session_id = s.session_id
                                        AND s2.last_request_start_time = s.last_request_start_time
                                )
                            )
                        )
                    FOR XML
                        PATH(''),
                        TYPE
                )
            FROM #sessions AS s
            WHERE
                recursion = 1
            OPTION (KEEPFIXED PLAN);

            CLOSE buffer_cursor;
            DEALLOCATE buffer_cursor;
        END;

        IF
            @get_plans >= 1
            AND @recursion = 1
            AND @output_column_list LIKE '%|[query_plan|]%' ESCAPE '|'
        BEGIN;
            DECLARE @live_plan BIT;
            SET @live_plan = ISNULL(CONVERT(BIT, SIGN(OBJECT_ID('sys.dm_exec_query_statistics_xml'))), 0)

            DECLARE plan_cursor
            CURSOR LOCAL FAST_FORWARD
            FOR
                SELECT
                    session_id,
                    request_id,
                    plan_handle,
                    statement_start_offset,
                    statement_end_offset
                FROM #sessions
                WHERE
                    recursion = 1
                    AND plan_handle IS NOT NULL
            OPTION (KEEPFIXED PLAN);

            OPEN plan_cursor;

            FETCH NEXT FROM plan_cursor
            INTO
                @session_id,
                @request_id,
                @plan_handle,
                @statement_start_offset,
                @statement_end_offset;

            --Wait up to 5 ms for a query plan, then give up
            SET LOCK_TIMEOUT 5;

            WHILE @@FETCH_STATUS = 0
            BEGIN;
                DECLARE @query_plan XML;
                SET @query_plan = NULL;

                IF @live_plan = 1
                BEGIN;
                    BEGIN TRY;
                        SELECT
                            @query_plan = x.query_plan
                        FROM sys.dm_exec_query_statistics_xml(@session_id) AS x;

                        IF
                            @query_plan IS NOT NULL
                            AND EXISTS
                            (
                                SELECT
                                    *
                                FROM sys.dm_exec_requests AS r
                                WHERE
                                    r.session_id = @session_id
                                    AND r.request_id = @request_id
                                    AND r.plan_handle = @plan_handle
                                    AND r.statement_start_offset = @statement_start_offset
                                    AND r.statement_end_offset = @statement_end_offset
                            )
                        BEGIN;
                            UPDATE s
                            SET
                                s.query_plan = @query_plan
                            FROM #sessions AS s
                            WHERE
                                s.session_id = @session_id
                                AND s.request_id = @request_id
                                AND s.recursion = 1
                            OPTION (KEEPFIXED PLAN);
                        END;
                    END TRY
                    BEGIN CATCH;
                        SET @query_plan = NULL;
                    END CATCH;
                END;

                IF @query_plan IS NULL
                BEGIN;
                    BEGIN TRY;
                        UPDATE s
                        SET
                            s.query_plan =
                            (
                                SELECT
                                    CONVERT(xml, query_plan)
                                FROM sys.dm_exec_text_query_plan
                                (
                                    @plan_handle,
                                    CASE @get_plans
                                        WHEN 1 THEN
                                            @statement_start_offset
                                        ELSE
                                            0
                                    END,
                                    CASE @get_plans
                                        WHEN 1 THEN
                                            @statement_end_offset
                                        ELSE
                                            -1
                                    END
                                )
                            )
                        FROM #sessions AS s
                        WHERE
                            s.session_id = @session_id
                            AND s.request_id = @request_id
                            AND s.recursion = 1
                        OPTION (KEEPFIXED PLAN);
                    END TRY
                    BEGIN CATCH;
                        IF ERROR_NUMBER() = 6335
                        BEGIN;
                            UPDATE s
                            SET
                                s.query_plan =
                                (
                                    SELECT
                                        N'--' + NCHAR(13) + NCHAR(10) +
                                        N'-- Could not render showplan due to XML data type limitations. ' + NCHAR(13) + NCHAR(10) +
                                        N'-- To see the graphical plan save the XML below as a .SQLPLAN file and re-open in SSMS.' + NCHAR(13) + NCHAR(10) +
                                        N'--' + NCHAR(13) + NCHAR(10) +
                                            REPLACE(qp.query_plan, N'<RelOp', NCHAR(13)+NCHAR(10)+N'<RelOp') +
                                            NCHAR(13) + NCHAR(10) + N'--' COLLATE Latin1_General_Bin2 AS [processing-instruction(query_plan)]
                                    FROM sys.dm_exec_text_query_plan
                                    (
                                        @plan_handle,
                                        CASE @get_plans
                                            WHEN 1 THEN
                                                @statement_start_offset
                                            ELSE
                                                0
                                        END,
                                        CASE @get_plans
                                            WHEN 1 THEN
                                                @statement_end_offset
                                            ELSE
                                                -1
                                        END
                                    ) AS qp
                                    FOR XML
                                        PATH(''),
                                        TYPE
                                )
                            FROM #sessions AS s
                            WHERE
                                s.session_id = @session_id
                                AND s.request_id = @request_id
                                AND s.recursion = 1
                            OPTION (KEEPFIXED PLAN);
                        END;
                        ELSE
                        BEGIN;
                            UPDATE s
                            SET
                                s.query_plan =
                                    CASE ERROR_NUMBER()
                                        WHEN 1222 THEN '<timeout_exceeded />'
                                        ELSE '<error message="' + ERROR_MESSAGE() + '" />'
                                    END
                            FROM #sessions AS s
                            WHERE
                                s.session_id = @session_id
                                AND s.request_id = @request_id
                                AND s.recursion = 1
                            OPTION (KEEPFIXED PLAN);
                        END;
                    END CATCH;
                END;

                FETCH NEXT FROM plan_cursor
                INTO
                    @session_id,
                    @request_id,
                    @plan_handle,
                    @statement_start_offset,
                    @statement_end_offset;
            END;

            --Return this to the default
            SET LOCK_TIMEOUT -1;

            CLOSE plan_cursor;
            DEALLOCATE plan_cursor;
        END;

        IF
            @get_locks = 1
            AND @recursion = 1
            AND @output_column_list LIKE '%|[locks|]%' ESCAPE '|'
        BEGIN;
            DECLARE locks_cursor
            CURSOR LOCAL FAST_FORWARD
            FOR
                SELECT DISTINCT
                    database_name
                FROM #locks
                WHERE
                    EXISTS
                    (
                        SELECT *
                        FROM #sessions AS s
                        WHERE
                            s.session_id = #locks.session_id
                            AND recursion = 1
                    )
                    AND database_name <> '(null)'
                OPTION (KEEPFIXED PLAN);

            OPEN locks_cursor;

            FETCH NEXT FROM locks_cursor
            INTO
                @database_name;

            WHILE @@FETCH_STATUS = 0
            BEGIN;
                BEGIN TRY;
                    SET @sql_n = CONVERT(NVARCHAR(MAX), N'') + N'
                        UPDATE l
                        SET
                            object_name =
                                REPLACE
                                (
                                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                        o.name COLLATE Latin1_General_Bin2,
                                        NCHAR(31),N''?''),NCHAR(30),N''?''),NCHAR(29),N''?''),NCHAR(28),N''?''),NCHAR(27),N''?''),NCHAR(26),N''?''),NCHAR(25),N''?''),NCHAR(24),N''?''),NCHAR(23),N''?''),NCHAR(22),N''?''),
                                        NCHAR(21),N''?''),NCHAR(20),N''?''),NCHAR(19),N''?''),NCHAR(18),N''?''),NCHAR(17),N''?''),NCHAR(16),N''?''),NCHAR(15),N''?''),NCHAR(14),N''?''),NCHAR(12),N''?''),
                                        NCHAR(11),N''?''),NCHAR(8),N''?''),NCHAR(7),N''?''),NCHAR(6),N''?''),NCHAR(5),N''?''),NCHAR(4),N''?''),NCHAR(3),N''?''),NCHAR(2),N''?''),NCHAR(1),N''?''),
                                    NCHAR(0),
                                    N''''
                                ),
                            index_name =
                                REPLACE
                                (
                                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                        i.name COLLATE Latin1_General_Bin2,
                                        NCHAR(31),N''?''),NCHAR(30),N''?''),NCHAR(29),N''?''),NCHAR(28),N''?''),NCHAR(27),N''?''),NCHAR(26),N''?''),NCHAR(25),N''?''),NCHAR(24),N''?''),NCHAR(23),N''?''),NCHAR(22),N''?''),
                                        NCHAR(21),N''?''),NCHAR(20),N''?''),NCHAR(19),N''?''),NCHAR(18),N''?''),NCHAR(17),N''?''),NCHAR(16),N''?''),NCHAR(15),N''?''),NCHAR(14),N''?''),NCHAR(12),N''?''),
                                        NCHAR(11),N''?''),NCHAR(8),N''?''),NCHAR(7),N''?''),NCHAR(6),N''?''),NCHAR(5),N''?''),NCHAR(4),N''?''),NCHAR(3),N''?''),NCHAR(2),N''?''),NCHAR(1),N''?''),
                                    NCHAR(0),
                                    N''''
                                ),
                            schema_name =
                                REPLACE
                                (
                                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                        s.name COLLATE Latin1_General_Bin2,
                                        NCHAR(31),N''?''),NCHAR(30),N''?''),NCHAR(29),N''?''),NCHAR(28),N''?''),NCHAR(27),N''?''),NCHAR(26),N''?''),NCHAR(25),N''?''),NCHAR(24),N''?''),NCHAR(23),N''?''),NCHAR(22),N''?''),
                                        NCHAR(21),N''?''),NCHAR(20),N''?''),NCHAR(19),N''?''),NCHAR(18),N''?''),NCHAR(17),N''?''),NCHAR(16),N''?''),NCHAR(15),N''?''),NCHAR(14),N''?''),NCHAR(12),N''?''),
                                        NCHAR(11),N''?''),NCHAR(8),N''?''),NCHAR(7),N''?''),NCHAR(6),N''?''),NCHAR(5),N''?''),NCHAR(4),N''?''),NCHAR(3),N''?''),NCHAR(2),N''?''),NCHAR(1),N''?''),
                                    NCHAR(0),
                                    N''''
                                ),
                            principal_name =
                                REPLACE
                                (
                                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                        dp.name COLLATE Latin1_General_Bin2,
                                        NCHAR(31),N''?''),NCHAR(30),N''?''),NCHAR(29),N''?''),NCHAR(28),N''?''),NCHAR(27),N''?''),NCHAR(26),N''?''),NCHAR(25),N''?''),NCHAR(24),N''?''),NCHAR(23),N''?''),NCHAR(22),N''?''),
                                        NCHAR(21),N''?''),NCHAR(20),N''?''),NCHAR(19),N''?''),NCHAR(18),N''?''),NCHAR(17),N''?''),NCHAR(16),N''?''),NCHAR(15),N''?''),NCHAR(14),N''?''),NCHAR(12),N''?''),
                                        NCHAR(11),N''?''),NCHAR(8),N''?''),NCHAR(7),N''?''),NCHAR(6),N''?''),NCHAR(5),N''?''),NCHAR(4),N''?''),NCHAR(3),N''?''),NCHAR(2),N''?''),NCHAR(1),N''?''),
                                    NCHAR(0),
                                    N''''
                                )
                        FROM #locks AS l
                        LEFT OUTER JOIN ' + QUOTENAME(@database_name) + N'.sys.allocation_units AS au ON
                            au.allocation_unit_id = l.allocation_unit_id
                        LEFT OUTER JOIN ' + QUOTENAME(@database_name) + N'.sys.partitions AS p ON
                            p.hobt_id =
                                COALESCE
                                (
                                    l.hobt_id,
                                    CASE
                                        WHEN au.type IN (1, 3) THEN au.container_id
                                        ELSE NULL
                                    END
                                )
                        LEFT OUTER JOIN ' + QUOTENAME(@database_name) + N'.sys.partitions AS p1 ON
                            l.hobt_id IS NULL
                            AND au.type = 2
                            AND p1.partition_id = au.container_id
                        LEFT OUTER JOIN ' + QUOTENAME(@database_name) + N'.sys.objects AS o ON
                            o.object_id = COALESCE(l.object_id, p.object_id, p1.object_id)
                        LEFT OUTER JOIN ' + QUOTENAME(@database_name) + N'.sys.indexes AS i ON
                            i.object_id = COALESCE(l.object_id, p.object_id, p1.object_id)
                            AND i.index_id = COALESCE(l.index_id, p.index_id, p1.index_id)
                        LEFT OUTER JOIN ' + QUOTENAME(@database_name) + N'.sys.schemas AS s ON
                            s.schema_id = COALESCE(l.schema_id, o.schema_id)
                        LEFT OUTER JOIN ' + QUOTENAME(@database_name) + N'.sys.database_principals AS dp ON
                            dp.principal_id = l.principal_id
                        WHERE
                            l.database_name = @database_name
                        OPTION (KEEPFIXED PLAN); ';

                    EXEC sp_executesql
                        @sql_n,
                        N'@database_name sysname',
                        @database_name;
                END TRY
                BEGIN CATCH;
                    UPDATE #locks
                    SET
                        query_error =
                            REPLACE
                            (
                                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                    CONVERT
                                    (
                                        NVARCHAR(MAX),
                                        ERROR_MESSAGE() COLLATE Latin1_General_Bin2
                                    ),
                                    NCHAR(31),N'?'),NCHAR(30),N'?'),NCHAR(29),N'?'),NCHAR(28),N'?'),NCHAR(27),N'?'),NCHAR(26),N'?'),NCHAR(25),N'?'),NCHAR(24),N'?'),NCHAR(23),N'?'),NCHAR(22),N'?'),
                                    NCHAR(21),N'?'),NCHAR(20),N'?'),NCHAR(19),N'?'),NCHAR(18),N'?'),NCHAR(17),N'?'),NCHAR(16),N'?'),NCHAR(15),N'?'),NCHAR(14),N'?'),NCHAR(12),N'?'),
                                    NCHAR(11),N'?'),NCHAR(8),N'?'),NCHAR(7),N'?'),NCHAR(6),N'?'),NCHAR(5),N'?'),NCHAR(4),N'?'),NCHAR(3),N'?'),NCHAR(2),N'?'),NCHAR(1),N'?'),
                                NCHAR(0),
                                N''
                            )
                    WHERE
                        database_name = @database_name
                    OPTION (KEEPFIXED PLAN);
                END CATCH;

                FETCH NEXT FROM locks_cursor
                INTO
                    @database_name;
            END;

            CLOSE locks_cursor;
            DEALLOCATE locks_cursor;

            CREATE CLUSTERED INDEX IX_SRD ON #locks (session_id, request_id, database_name);

            UPDATE s
            SET
                s.locks =
                (
                    SELECT
                        REPLACE
                        (
                            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                CONVERT
                                (
                                    NVARCHAR(MAX),
                                    l1.database_name COLLATE Latin1_General_Bin2
                                ),
                                NCHAR(31),N'?'),NCHAR(30),N'?'),NCHAR(29),N'?'),NCHAR(28),N'?'),NCHAR(27),N'?'),NCHAR(26),N'?'),NCHAR(25),N'?'),NCHAR(24),N'?'),NCHAR(23),N'?'),NCHAR(22),N'?'),
                                NCHAR(21),N'?'),NCHAR(20),N'?'),NCHAR(19),N'?'),NCHAR(18),N'?'),NCHAR(17),N'?'),NCHAR(16),N'?'),NCHAR(15),N'?'),NCHAR(14),N'?'),NCHAR(12),N'?'),
                                NCHAR(11),N'?'),NCHAR(8),N'?'),NCHAR(7),N'?'),NCHAR(6),N'?'),NCHAR(5),N'?'),NCHAR(4),N'?'),NCHAR(3),N'?'),NCHAR(2),N'?'),NCHAR(1),N'?'),
                            NCHAR(0),
                            N''
                        ) AS [Database/@name],
                        MIN(l1.query_error) AS [Database/@query_error],
                        (
                            SELECT
                                l2.request_mode AS [Lock/@request_mode],
                                l2.request_status AS [Lock/@request_status],
                                COUNT(*) AS [Lock/@request_count]
                            FROM #locks AS l2
                            WHERE
                                l1.session_id = l2.session_id
                                AND l1.request_id = l2.request_id
                                AND l2.database_name = l1.database_name
                                AND l2.resource_type = 'DATABASE'
                            GROUP BY
                                l2.request_mode,
                                l2.request_status
                            FOR XML
                                PATH(''),
                                TYPE
                        ) AS [Database/Locks],
                        (
                            SELECT
                                COALESCE(l3.object_name, '(null)') AS [Object/@name],
                                l3.schema_name AS [Object/@schema_name],
                                (
                                    SELECT
                                        l4.resource_type AS [Lock/@resource_type],
                                        l4.page_type AS [Lock/@page_type],
                                        l4.index_name AS [Lock/@index_name],
                                        CASE
                                            WHEN l4.object_name IS NULL THEN l4.schema_name
                                            ELSE NULL
                                        END AS [Lock/@schema_name],
                                        l4.principal_name AS [Lock/@principal_name],
                                        REPLACE
                                        (
                                            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                                l4.resource_description COLLATE Latin1_General_Bin2,
                                                NCHAR(31),N'?'),NCHAR(30),N'?'),NCHAR(29),N'?'),NCHAR(28),N'?'),NCHAR(27),N'?'),NCHAR(26),N'?'),NCHAR(25),N'?'),NCHAR(24),N'?'),NCHAR(23),N'?'),NCHAR(22),N'?'),
                                                NCHAR(21),N'?'),NCHAR(20),N'?'),NCHAR(19),N'?'),NCHAR(18),N'?'),NCHAR(17),N'?'),NCHAR(16),N'?'),NCHAR(15),N'?'),NCHAR(14),N'?'),NCHAR(12),N'?'),
                                                NCHAR(11),N'?'),NCHAR(8),N'?'),NCHAR(7),N'?'),NCHAR(6),N'?'),NCHAR(5),N'?'),NCHAR(4),N'?'),NCHAR(3),N'?'),NCHAR(2),N'?'),NCHAR(1),N'?'),
                                            NCHAR(0),
                                            N''
                                        ) AS [Lock/@resource_description],
                                        l4.request_mode AS [Lock/@request_mode],
                                        l4.request_status AS [Lock/@request_status],
                                        SUM(l4.request_count) AS [Lock/@request_count]
                                    FROM #locks AS l4
                                    WHERE
                                        l4.session_id = l3.session_id
                                        AND l4.request_id = l3.request_id
                                        AND l3.database_name = l4.database_name
                                        AND COALESCE(l3.object_name, '(null)') = COALESCE(l4.object_name, '(null)')
                                        AND COALESCE(l3.schema_name, '') = COALESCE(l4.schema_name, '')
                                        AND l4.resource_type <> 'DATABASE'
                                    GROUP BY
                                        l4.resource_type,
                                        l4.page_type,
                                        l4.index_name,
                                        CASE
                                            WHEN l4.object_name IS NULL THEN l4.schema_name
                                            ELSE NULL
                                        END,
                                        l4.principal_name,
                                        l4.resource_description,
                                        l4.request_mode,
                                        l4.request_status
                                    FOR XML
                                        PATH(''),
                                        TYPE
                                ) AS [Object/Locks]
                            FROM #locks AS l3
                            WHERE
                                l3.session_id = l1.session_id
                                AND l3.request_id = l1.request_id
                                AND l3.database_name = l1.database_name
                                AND l3.resource_type <> 'DATABASE'
                            GROUP BY
                                l3.session_id,
                                l3.request_id,
                                l3.database_name,
                                COALESCE(l3.object_name, '(null)'),
                                l3.schema_name
                            FOR XML
                                PATH(''),
                                TYPE
                        ) AS [Database/Objects]
                    FROM #locks AS l1
                    WHERE
                        l1.session_id = s.session_id
                        AND l1.request_id = s.request_id
                        AND l1.start_time IN (s.start_time, s.last_request_start_time)
                        AND s.recursion = 1
                    GROUP BY
                        l1.session_id,
                        l1.request_id,
                        l1.database_name
                    FOR XML
                        PATH(''),
                        TYPE
                )
            FROM #sessions s
            OPTION (KEEPFIXED PLAN);
        END;

        IF
            @find_block_leaders = 1
            AND @recursion = 1
            AND @output_column_list LIKE '%|[blocked_session_count|]%' ESCAPE '|'
        BEGIN;
            WITH
            blockers AS
            (
                SELECT
                    session_id,
                    session_id AS top_level_session_id,
                    CONVERT(VARCHAR(8000), '.' + CONVERT(VARCHAR(8000), session_id) + '.') AS the_path
                FROM #sessions
                WHERE
                    recursion = 1

                UNION ALL

                SELECT
                    s.session_id,
                    b.top_level_session_id,
                    CONVERT(VARCHAR(8000), b.the_path + CONVERT(VARCHAR(8000), s.session_id) + '.') AS the_path
                FROM blockers AS b
                JOIN #sessions AS s ON
                    s.blocking_session_id = b.session_id
                    AND s.recursion = 1
                    AND b.the_path NOT LIKE '%.' + CONVERT(VARCHAR(8000), s.session_id) + '.%' COLLATE Latin1_General_Bin2
            )
            UPDATE s
            SET
                s.blocked_session_count = x.blocked_session_count
            FROM #sessions AS s
            JOIN
            (
                SELECT
                    b.top_level_session_id AS session_id,
                    COUNT(*) - 1 AS blocked_session_count
                FROM blockers AS b
                GROUP BY
                    b.top_level_session_id
            ) x ON
                s.session_id = x.session_id
            WHERE
                s.recursion = 1;
        END;

        IF
            @get_task_info = 2
            AND @output_column_list LIKE '%|[additional_info|]%' ESCAPE '|'
            AND @recursion = 1
        BEGIN;
            CREATE TABLE #blocked_requests
            (
                session_id SMALLINT NOT NULL,
                request_id INT NOT NULL,
                database_name sysname NOT NULL,
                object_id INT,
                hobt_id BIGINT,
                schema_id INT,
                schema_name sysname NULL,
                object_name sysname NULL,
                query_error NVARCHAR(2048),
                PRIMARY KEY (database_name, session_id, request_id)
            );

            CREATE STATISTICS s_database_name ON #blocked_requests (database_name)
            WITH SAMPLE 0 ROWS, NORECOMPUTE;
            CREATE STATISTICS s_schema_name ON #blocked_requests (schema_name)
            WITH SAMPLE 0 ROWS, NORECOMPUTE;
            CREATE STATISTICS s_object_name ON #blocked_requests (object_name)
            WITH SAMPLE 0 ROWS, NORECOMPUTE;
            CREATE STATISTICS s_query_error ON #blocked_requests (query_error)
            WITH SAMPLE 0 ROWS, NORECOMPUTE;
       
            INSERT #blocked_requests
            (
                session_id,
                request_id,
                database_name,
                object_id,
                hobt_id,
                schema_id
            )
            SELECT
                session_id,
                request_id,
                database_name,
                object_id,
                hobt_id,
                CONVERT(INT, SUBSTRING(schema_node, CHARINDEX(' = ', schema_node) + 3, LEN(schema_node))) AS schema_id
            FROM
            (
                SELECT
                    session_id,
                    request_id,
                    agent_nodes.agent_node.value('(database_name/text())[1]', 'sysname') AS database_name,
                    agent_nodes.agent_node.value('(object_id/text())[1]', 'int') AS object_id,
                    agent_nodes.agent_node.value('(hobt_id/text())[1]', 'bigint') AS hobt_id,
                    agent_nodes.agent_node.value('(metadata_resource/text()[.="SCHEMA"]/../../metadata_class_id/text())[1]', 'varchar(100)') AS schema_node
                FROM #sessions AS s
                CROSS APPLY s.additional_info.nodes('//block_info') AS agent_nodes (agent_node)
                WHERE
                    s.recursion = 1
            ) AS t
            WHERE
                t.database_name IS NOT NULL
                AND
                (
                    t.object_id IS NOT NULL
                    OR t.hobt_id IS NOT NULL
                    OR t.schema_node IS NOT NULL
                );
           
            DECLARE blocks_cursor
            CURSOR LOCAL FAST_FORWARD
            FOR
                SELECT DISTINCT
                    database_name
                FROM #blocked_requests;
               
            OPEN blocks_cursor;
           
            FETCH NEXT FROM blocks_cursor
            INTO
                @database_name;
           
            WHILE @@FETCH_STATUS = 0
            BEGIN;
                BEGIN TRY;
                    SET @sql_n =
                        CONVERT(NVARCHAR(MAX), N'') + N'
                        UPDATE b
                        SET
                            b.schema_name =
                                REPLACE
                                (
                                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                        s.name COLLATE Latin1_General_Bin2,
                                        NCHAR(31),N''?''),NCHAR(30),N''?''),NCHAR(29),N''?''),NCHAR(28),N''?''),NCHAR(27),N''?''),NCHAR(26),N''?''),NCHAR(25),N''?''),NCHAR(24),N''?''),NCHAR(23),N''?''),NCHAR(22),N''?''),
                                        NCHAR(21),N''?''),NCHAR(20),N''?''),NCHAR(19),N''?''),NCHAR(18),N''?''),NCHAR(17),N''?''),NCHAR(16),N''?''),NCHAR(15),N''?''),NCHAR(14),N''?''),NCHAR(12),N''?''),
                                        NCHAR(11),N''?''),NCHAR(8),N''?''),NCHAR(7),N''?''),NCHAR(6),N''?''),NCHAR(5),N''?''),NCHAR(4),N''?''),NCHAR(3),N''?''),NCHAR(2),N''?''),NCHAR(1),N''?''),
                                    NCHAR(0),
                                    N''''
                                ),
                            b.object_name =
                                REPLACE
                                (
                                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                        o.name COLLATE Latin1_General_Bin2,
                                        NCHAR(31),N''?''),NCHAR(30),N''?''),NCHAR(29),N''?''),NCHAR(28),N''?''),NCHAR(27),N''?''),NCHAR(26),N''?''),NCHAR(25),N''?''),NCHAR(24),N''?''),NCHAR(23),N''?''),NCHAR(22),N''?''),
                                        NCHAR(21),N''?''),NCHAR(20),N''?''),NCHAR(19),N''?''),NCHAR(18),N''?''),NCHAR(17),N''?''),NCHAR(16),N''?''),NCHAR(15),N''?''),NCHAR(14),N''?''),NCHAR(12),N''?''),
                                        NCHAR(11),N''?''),NCHAR(8),N''?''),NCHAR(7),N''?''),NCHAR(6),N''?''),NCHAR(5),N''?''),NCHAR(4),N''?''),NCHAR(3),N''?''),NCHAR(2),N''?''),NCHAR(1),N''?''),
                                    NCHAR(0),
                                    N''''
                                )
                        FROM #blocked_requests AS b
                        LEFT OUTER JOIN ' + QUOTENAME(@database_name) + N'.sys.partitions AS p ON
                            p.hobt_id = b.hobt_id
                        LEFT OUTER JOIN ' + QUOTENAME(@database_name) + N'.sys.objects AS o ON
                            o.object_id = COALESCE(p.object_id, b.object_id)
                        LEFT OUTER JOIN ' + QUOTENAME(@database_name) + N'.sys.schemas AS s ON
                            s.schema_id = COALESCE(o.schema_id, b.schema_id)
                        WHERE
                            b.database_name = @database_name; ';
                   
                    EXEC sp_executesql
                        @sql_n,
                        N'@database_name sysname',
                        @database_name;
                END TRY
                BEGIN CATCH;
                    UPDATE #blocked_requests
                    SET
                        query_error =
                            REPLACE
                            (
                                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                    CONVERT
                                    (
                                        NVARCHAR(MAX),
                                        ERROR_MESSAGE() COLLATE Latin1_General_Bin2
                                    ),
                                    NCHAR(31),N'?'),NCHAR(30),N'?'),NCHAR(29),N'?'),NCHAR(28),N'?'),NCHAR(27),N'?'),NCHAR(26),N'?'),NCHAR(25),N'?'),NCHAR(24),N'?'),NCHAR(23),N'?'),NCHAR(22),N'?'),
                                    NCHAR(21),N'?'),NCHAR(20),N'?'),NCHAR(19),N'?'),NCHAR(18),N'?'),NCHAR(17),N'?'),NCHAR(16),N'?'),NCHAR(15),N'?'),NCHAR(14),N'?'),NCHAR(12),N'?'),
                                    NCHAR(11),N'?'),NCHAR(8),N'?'),NCHAR(7),N'?'),NCHAR(6),N'?'),NCHAR(5),N'?'),NCHAR(4),N'?'),NCHAR(3),N'?'),NCHAR(2),N'?'),NCHAR(1),N'?'),
                                NCHAR(0),
                                N''
                            )
                    WHERE
                        database_name = @database_name;
                END CATCH;

                FETCH NEXT FROM blocks_cursor
                INTO
                    @database_name;
            END;
           
            CLOSE blocks_cursor;
            DEALLOCATE blocks_cursor;
           
            UPDATE s
            SET
                additional_info.modify
                ('
                    insert <schema_name>{sql:column("b.schema_name")}</schema_name>
                    as last
                    into (/additional_info/block_info)[1]
                ')
            FROM #sessions AS s
            INNER JOIN #blocked_requests AS b ON
                b.session_id = s.session_id
                AND b.request_id = s.request_id
                AND s.recursion = 1
            WHERE
                b.schema_name IS NOT NULL;

            UPDATE s
            SET
                additional_info.modify
                ('
                    insert <object_name>{sql:column("b.object_name")}</object_name>
                    as last
                    into (/additional_info/block_info)[1]
                ')
            FROM #sessions AS s
            INNER JOIN #blocked_requests AS b ON
                b.session_id = s.session_id
                AND b.request_id = s.request_id
                AND s.recursion = 1
            WHERE
                b.object_name IS NOT NULL;

            UPDATE s
            SET
                additional_info.modify
                ('
                    insert <query_error>{sql:column("b.query_error")}</query_error>
                    as last
                    into (/additional_info/block_info)[1]
                ')
            FROM #sessions AS s
            INNER JOIN #blocked_requests AS b ON
                b.session_id = s.session_id
                AND b.request_id = s.request_id
                AND s.recursion = 1
            WHERE
                b.query_error IS NOT NULL;
        END;

        IF
            @output_column_list LIKE '%|[program_name|]%' ESCAPE '|'
            AND @output_column_list LIKE '%|[additional_info|]%' ESCAPE '|'
            AND @recursion = 1
            AND DB_ID('msdb') IS NOT NULL
        BEGIN;
            SET @sql_n =
                N'BEGIN TRY;
                    DECLARE @job_name sysname;
                    SET @job_name = NULL;
                    DECLARE @step_name sysname;
                    SET @step_name = NULL;

                    SELECT
                        @job_name =
                            REPLACE
                            (
                                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                    j.name,
                                    NCHAR(31),N''?''),NCHAR(30),N''?''),NCHAR(29),N''?''),NCHAR(28),N''?''),NCHAR(27),N''?''),NCHAR(26),N''?''),NCHAR(25),N''?''),NCHAR(24),N''?''),NCHAR(23),N''?''),NCHAR(22),N''?''),
                                    NCHAR(21),N''?''),NCHAR(20),N''?''),NCHAR(19),N''?''),NCHAR(18),N''?''),NCHAR(17),N''?''),NCHAR(16),N''?''),NCHAR(15),N''?''),NCHAR(14),N''?''),NCHAR(12),N''?''),
                                    NCHAR(11),N''?''),NCHAR(8),N''?''),NCHAR(7),N''?''),NCHAR(6),N''?''),NCHAR(5),N''?''),NCHAR(4),N''?''),NCHAR(3),N''?''),NCHAR(2),N''?''),NCHAR(1),N''?''),
                                NCHAR(0),
                                N''?''
                            ),
                        @step_name =
                            REPLACE
                            (
                                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                    s.step_name,
                                    NCHAR(31),N''?''),NCHAR(30),N''?''),NCHAR(29),N''?''),NCHAR(28),N''?''),NCHAR(27),N''?''),NCHAR(26),N''?''),NCHAR(25),N''?''),NCHAR(24),N''?''),NCHAR(23),N''?''),NCHAR(22),N''?''),
                                    NCHAR(21),N''?''),NCHAR(20),N''?''),NCHAR(19),N''?''),NCHAR(18),N''?''),NCHAR(17),N''?''),NCHAR(16),N''?''),NCHAR(15),N''?''),NCHAR(14),N''?''),NCHAR(12),N''?''),
                                    NCHAR(11),N''?''),NCHAR(8),N''?''),NCHAR(7),N''?''),NCHAR(6),N''?''),NCHAR(5),N''?''),NCHAR(4),N''?''),NCHAR(3),N''?''),NCHAR(2),N''?''),NCHAR(1),N''?''),
                                NCHAR(0),
                                N''?''
                            )
                    FROM msdb.dbo.sysjobs AS j
                    INNER JOIN msdb.dbo.sysjobsteps AS s ON
                        j.job_id = s.job_id
                    WHERE
                        j.job_id = @job_id
                        AND s.step_id = @step_id;

                    IF @job_name IS NOT NULL
                    BEGIN;
                        UPDATE s
                        SET
                            additional_info.modify
                            (''
                                insert text{sql:variable("@job_name")}
                                into (/additional_info/agent_job_info/job_name)[1]
                            '')
                        FROM #sessions AS s
                        WHERE
                            s.session_id = @session_id
                            AND s.recursion = 1
                        OPTION (KEEPFIXED PLAN);
                       
                        UPDATE s
                        SET
                            additional_info.modify
                            (''
                                insert text{sql:variable("@step_name")}
                                into (/additional_info/agent_job_info/step_name)[1]
                            '')
                        FROM #sessions AS s
                        WHERE
                            s.session_id = @session_id
                            AND s.recursion = 1
                        OPTION (KEEPFIXED PLAN);
                    END;
                END TRY
                BEGIN CATCH;
                    DECLARE @msdb_error_message NVARCHAR(256);
                    SET @msdb_error_message = ERROR_MESSAGE();
               
                    UPDATE s
                    SET
                        additional_info.modify
                        (''
                            insert <msdb_query_error>{sql:variable("@msdb_error_message")}</msdb_query_error>
                            as last
                            into (/additional_info/agent_job_info)[1]
                        '')
                    FROM #sessions AS s
                    WHERE
                        s.session_id = @session_id
                        AND s.recursion = 1
                    OPTION (KEEPFIXED PLAN);
                END CATCH;'

            DECLARE @job_id UNIQUEIDENTIFIER;
            DECLARE @step_id INT;

            DECLARE agent_cursor
            CURSOR LOCAL FAST_FORWARD
            FOR
                SELECT
                    s.session_id,
                    agent_nodes.agent_node.value('(job_id/text())[1]', 'uniqueidentifier') AS job_id,
                    agent_nodes.agent_node.value('(step_id/text())[1]', 'int') AS step_id
                FROM #sessions AS s
                CROSS APPLY s.additional_info.nodes('//agent_job_info') AS agent_nodes (agent_node)
                WHERE
                    s.recursion = 1
            OPTION (KEEPFIXED PLAN);
           
            OPEN agent_cursor;

            FETCH NEXT FROM agent_cursor
            INTO
                @session_id,
                @job_id,
                @step_id;

            WHILE @@FETCH_STATUS = 0
            BEGIN;
                EXEC sp_executesql
                    @sql_n,
                    N'@job_id UNIQUEIDENTIFIER, @step_id INT, @session_id SMALLINT',
                    @job_id, @step_id, @session_id

                FETCH NEXT FROM agent_cursor
                INTO
                    @session_id,
                    @job_id,
                    @step_id;
            END;

            CLOSE agent_cursor;
            DEALLOCATE agent_cursor;
        END;
       
        IF
            @delta_interval > 0
            AND @recursion <> 1
        BEGIN;
            SET @recursion = 1;

            DECLARE @delay_time CHAR(12);
            SET @delay_time = CONVERT(VARCHAR, DATEADD(second, @delta_interval, 0), 114);
            WAITFOR DELAY @delay_time;

            GOTO REDO;
        END;
    END;

    DECLARE
        @num_data_threshold MONEY,
        @num_col_fmt NVARCHAR(MAX),
        @num_delta_col_fmt NVARCHAR(MAX);

    SET @num_data_threshold = 919919919919919;
    SET @num_col_fmt =
        CASE @format_output
            WHEN 1 THEN N'
                CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, [col_name]))) OVER() - LEN(CONVERT(VARCHAR, [col_name]))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, CASE WHEN [col_name] > @num_data_threshold THEN @num_data_threshold ELSE [col_name] END), 1), 19)) AS '
            WHEN 2 THEN N'
                CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, CASE WHEN [col_name] > @num_data_threshold THEN @num_data_threshold ELSE [col_name] END), 1), 19)) AS '
            ELSE N''
        END + N'[col_name], ';
    SET @num_delta_col_fmt =
        N'
        CASE
            WHEN
                first_request_start_time = last_request_start_time
                AND num_events = 2
                AND [col_name] >= 0
                    THEN ' +
                    CASE @format_output
                        WHEN 1 THEN N'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, [col_name]))) OVER() - LEN(CONVERT(VARCHAR, [col_name]))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, CASE WHEN [col_name] > @num_data_threshold THEN @num_data_threshold ELSE [col_name] END), 1), 19)) '
                        WHEN 2 THEN N'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, CASE WHEN [col_name] > @num_data_threshold THEN @num_data_threshold ELSE [col_name] END), 1), 19)) '
                        ELSE N'[col_name] '
                    END + N'
            ELSE NULL
        END AS [col_name], ';

    SET @sql_n = CONVERT(NVARCHAR(MAX), N'') +
        --Outer column list
        CASE
            WHEN
                @destination_table <> ''
                AND @return_schema = 0
                    THEN N'INSERT ' + @destination_table + ' '
            ELSE N''
        END +
        N'SELECT ' +
            @output_column_list + N' ' +
        CASE @return_schema
            WHEN 1 THEN N'INTO #session_schema '
            ELSE N''
        END
        --End outer column list
        +
        --Inner column list
        N'
        FROM
        (
            SELECT
                session_id, ' +
                --[dd hh:mm:ss.mss]
                CASE
                    WHEN @format_output IN (1, 2) THEN
                        N'
                        CASE
                            WHEN elapsed_time < 0 THEN
                                RIGHT
                                (
                                    REPLICATE(''0'', max_elapsed_length) + CONVERT(VARCHAR, (-1 * elapsed_time) / 86400),
                                    max_elapsed_length
                                ) +
                                    RIGHT
                                    (
                                        CONVERT(VARCHAR, DATEADD(second, (-1 * elapsed_time), 0), 120),
                                        9
                                    ) +
                                    ''.000''
                            ELSE
                                RIGHT
                                (
                                    REPLICATE(''0'', max_elapsed_length) + CONVERT(VARCHAR, elapsed_time / 86400000),
                                    max_elapsed_length
                                ) +
                                    RIGHT
                                    (
                                        CONVERT(VARCHAR, DATEADD(second, elapsed_time / 1000, 0), 120),
                                        9
                                    ) +
                                    ''.'' +
                                    RIGHT(''000'' + CONVERT(VARCHAR, elapsed_time % 1000), 3)
                        END AS [dd hh:mm:ss.mss], '
                    ELSE
                        N''
                END +
                --[dd hh:mm:ss.mss (avg)] / avg_elapsed_time
                CASE
                    WHEN  @format_output IN (1, 2) THEN
                        N'
                        RIGHT
                        (
                            ''00'' + CONVERT(VARCHAR, avg_elapsed_time / 86400000),
                            2
                        ) +
                            RIGHT
                            (
                                CONVERT(VARCHAR, DATEADD(second, avg_elapsed_time / 1000, 0), 120),
                                9
                            ) +
                            ''.'' +
                            RIGHT(''000'' + CONVERT(VARCHAR, avg_elapsed_time % 1000), 3) AS [dd hh:mm:ss.mss (avg)], '
                    ELSE
                        N'avg_elapsed_time, '
                END +
                REPLACE(@num_col_fmt, N'[col_name]', N'physical_io') +
                REPLACE(@num_col_fmt, N'[col_name]', N'reads') +
                REPLACE(@num_col_fmt, N'[col_name]', N'physical_reads') +
                REPLACE(@num_col_fmt, N'[col_name]', N'writes') +
                REPLACE(@num_col_fmt, N'[col_name]', N'tempdb_allocations') +
                REPLACE(@num_col_fmt, N'[col_name]', N'tempdb_current') +
                REPLACE(@num_col_fmt, N'[col_name]', N'CPU') +
                REPLACE(@num_col_fmt, N'[col_name]', N'context_switches') +
                REPLACE(@num_col_fmt, N'[col_name]', N'used_memory') +
                REPLACE(@num_col_fmt, N'[col_name]', N'max_used_memory') +
                REPLACE(@num_col_fmt, N'[col_name]', N'requested_memory') +
                REPLACE(@num_col_fmt, N'[col_name]', N'granted_memory') +
                CASE
                    WHEN @output_column_list LIKE '%|_delta|]%' ESCAPE '|' THEN
                        REPLACE(@num_delta_col_fmt, N'[col_name]', N'physical_io_delta') +
                        REPLACE(@num_delta_col_fmt, N'[col_name]', N'reads_delta') +
                        REPLACE(@num_delta_col_fmt, N'[col_name]', N'physical_reads_delta') +
                        REPLACE(@num_delta_col_fmt, N'[col_name]', N'writes_delta') +
                        REPLACE(@num_delta_col_fmt, N'[col_name]', N'tempdb_allocations_delta') +
                        --this is the only one that can (legitimately) go negative
                        REPLACE(@num_delta_col_fmt, N'[col_name]', N'tempdb_current_delta') +
                        --CPU_delta
                        --leaving this one hardcoded, as there is a bit of different interaction here
                        N'
                        CASE
                            WHEN
                                first_request_start_time = last_request_start_time
                                AND num_events = 2
                                    THEN
                                        CASE
                                            WHEN
                                                thread_CPU_delta > CPU_delta
                                                AND thread_CPU_delta > 0
                                                    THEN ' +
                                                        CASE @format_output
                                                            WHEN 1 THEN N'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, thread_CPU_delta + CPU_delta))) OVER() - LEN(CONVERT(VARCHAR, thread_CPU_delta))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, CASE WHEN thread_CPU_delta > @num_data_threshold THEN @num_data_threshold ELSE thread_CPU_delta END), 1), 19)) '
                                                            WHEN 2 THEN N'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, CASE WHEN thread_CPU_delta > @num_data_threshold THEN @num_data_threshold ELSE thread_CPU_delta END), 1), 19)) '
                                                            ELSE N'thread_CPU_delta '
                                                        END + N'
                                            WHEN CPU_delta >= 0 THEN ' +
                                                CASE @format_output
                                                    WHEN 1 THEN N'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, thread_CPU_delta + CPU_delta))) OVER() - LEN(CONVERT(VARCHAR, CPU_delta))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, CASE WHEN CPU_delta > @num_data_threshold THEN @num_data_threshold ELSE CPU_delta END), 1), 19)) '
                                                    WHEN 2 THEN N'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, CASE WHEN CPU_delta > @num_data_threshold THEN @num_data_threshold ELSE CPU_delta END), 1), 19)) '
                                                    ELSE N'CPU_delta '
                                                END + N'
                                            ELSE NULL
                                        END
                            ELSE
                                NULL
                        END AS CPU_delta, ' +
                        REPLACE(@num_delta_col_fmt, N'[col_name]', N'context_switches_delta') +
                        REPLACE(@num_delta_col_fmt, N'[col_name]', N'used_memory_delta') +
                        REPLACE(@num_delta_col_fmt, N'[col_name]', N'max_used_memory_delta')
                    ELSE N''
                END + N'
                ' +
                REPLACE(@num_col_fmt, N'[col_name]', N'tasks') + N'
                status,
                wait_info,
                locks,
                tran_start_time,
                LEFT(tran_log_writes, LEN(tran_log_writes) - 1) AS tran_log_writes,
                implicit_tran, ' +
                REPLACE(@num_col_fmt, '[col_name]', 'open_tran_count') + N'
                ' +
                --sql_command
                CASE @format_output
                    WHEN 0 THEN N'REPLACE(REPLACE(CONVERT(NVARCHAR(MAX), sql_command), ''<?query --''+CHAR(13)+CHAR(10), ''''), CHAR(13)+CHAR(10)+''--?>'', '''') AS '
                    ELSE N''
                END + N'sql_command,
                ' +
                --sql_text
                CASE @format_output
                    WHEN 0 THEN N'REPLACE(REPLACE(CONVERT(NVARCHAR(MAX), sql_text), ''<?query --''+CHAR(13)+CHAR(10), ''''), CHAR(13)+CHAR(10)+''--?>'', '''') AS '
                    ELSE N''
                END + N'sql_text,
                query_plan,
                blocking_session_id, ' +
                REPLACE(@num_col_fmt, N'[col_name]', N'blocked_session_count') +
                REPLACE(@num_col_fmt, N'[col_name]', N'percent_complete') + N'
                host_name,
                login_name,
                database_name,
                program_name,
                additional_info,
                memory_info,
                start_time,
                login_time,
                CASE
                    WHEN status = N''sleeping'' THEN NULL
                    ELSE request_id
                END AS request_id,
                GETDATE() AS collection_time '
        --End inner column list
        +
        --Derived table and INSERT specification
            N'
            FROM
            (
                SELECT TOP(2147483647)
                    *,
                    CASE
                        MAX
                        (
                            LEN
                            (
                                CONVERT
                                (
                                    VARCHAR,
                                    CASE
                                        WHEN elapsed_time < 0 THEN
                                            (-1 * elapsed_time) / 86400
                                        ELSE
                                            elapsed_time / 86400000
                                    END
                                )
                            )
                        ) OVER ()
                            WHEN 1 THEN 2
                            ELSE
                                MAX
                                (
                                    LEN
                                    (
                                        CONVERT
                                        (
                                            VARCHAR,
                                            CASE
                                                WHEN elapsed_time < 0 THEN
                                                    (-1 * elapsed_time) / 86400
                                                ELSE
                                                    elapsed_time / 86400000
                                            END
                                        )
                                    )
                                ) OVER ()
                    END AS max_elapsed_length, ' +
                    CASE
                        WHEN @output_column_list LIKE '%|_delta|]%' ESCAPE '|' THEN
                            N'
                            MAX(physical_io * recursion) OVER (PARTITION BY session_id, request_id) +
                                MIN(physical_io * recursion) OVER (PARTITION BY session_id, request_id) AS physical_io_delta,
                            MAX(reads * recursion) OVER (PARTITION BY session_id, request_id) +
                                MIN(reads * recursion) OVER (PARTITION BY session_id, request_id) AS reads_delta,
                            MAX(physical_reads * recursion) OVER (PARTITION BY session_id, request_id) +
                                MIN(physical_reads * recursion) OVER (PARTITION BY session_id, request_id) AS physical_reads_delta,
                            MAX(writes * recursion) OVER (PARTITION BY session_id, request_id) +
                                MIN(writes * recursion) OVER (PARTITION BY session_id, request_id) AS writes_delta,
                            MAX(tempdb_allocations * recursion) OVER (PARTITION BY session_id, request_id) +
                                MIN(tempdb_allocations * recursion) OVER (PARTITION BY session_id, request_id) AS tempdb_allocations_delta,
                            MAX(tempdb_current * recursion) OVER (PARTITION BY session_id, request_id) +
                                MIN(tempdb_current * recursion) OVER (PARTITION BY session_id, request_id) AS tempdb_current_delta,
                            MAX(CPU * recursion) OVER (PARTITION BY session_id, request_id) +
                                MIN(CPU * recursion) OVER (PARTITION BY session_id, request_id) AS CPU_delta,
                            MAX(thread_CPU_snapshot * recursion) OVER (PARTITION BY session_id, request_id) +
                                MIN(thread_CPU_snapshot * recursion) OVER (PARTITION BY session_id, request_id) AS thread_CPU_delta,
                            MAX(context_switches * recursion) OVER (PARTITION BY session_id, request_id) +
                                MIN(context_switches * recursion) OVER (PARTITION BY session_id, request_id) AS context_switches_delta,
                            MAX(used_memory * recursion) OVER (PARTITION BY session_id, request_id) +
                                MIN(used_memory * recursion) OVER (PARTITION BY session_id, request_id) AS used_memory_delta,
                            MAX(max_used_memory * recursion) OVER (PARTITION BY session_id, request_id) +
                                MIN(max_used_memory * recursion) OVER (PARTITION BY session_id, request_id) AS max_used_memory_delta,
                            MIN(last_request_start_time) OVER (PARTITION BY session_id, request_id) AS first_request_start_time, '
                        ELSE N''
                    END + N'
                    COUNT(*) OVER (PARTITION BY session_id, request_id) AS num_events
                FROM #sessions AS s1 ' +
                CASE
                    WHEN @sort_order = '' THEN N''
                    ELSE
                        N'
                        ORDER BY ' +
                            CONVERT(NVARCHAR(MAX), @sort_order)
                END +
            N'
            ) AS s
            WHERE
                s.recursion = 1
        ) x
        OPTION (KEEPFIXED PLAN);
        ' +
        CASE @return_schema
            WHEN 1 THEN
                N'
                SET @schema =
                    ''CREATE TABLE <table_name> ( '' +
                        STUFF
                        (
                            (
                                SELECT
                                    '','' +
                                    QUOTENAME(COLUMN_NAME) + '' '' +
                                    DATA_TYPE +
                                    CASE
                                        WHEN DATA_TYPE LIKE ''%char'' THEN ''('' + COALESCE(NULLIF(CONVERT(VARCHAR, CHARACTER_MAXIMUM_LENGTH), ''-1''), ''max'') + '') ''
                                        ELSE '' ''
                                    END +
                                    CASE IS_NULLABLE
                                        WHEN ''NO'' THEN ''NOT ''
                                        ELSE ''''
                                    END + ''NULL'' AS [text()]
                                FROM tempdb.INFORMATION_SCHEMA.COLUMNS
                                WHERE
                                    TABLE_NAME = (SELECT name FROM tempdb.sys.objects WHERE object_id = OBJECT_ID(''tempdb..#session_schema''))
                                    ORDER BY
                                        ORDINAL_POSITION
                                FOR XML
                                    PATH('''')
                            ), +
                            1,
                            1,
                            ''''
                        ) +
                    '');''; '
            ELSE N''
        END;
        --End derived table and INSERT specification

    EXEC sp_executesql
        @sql_n,
        N'@num_data_threshold MONEY, @schema VARCHAR(MAX) OUTPUT',
        @num_data_threshold, @schema OUTPUT;
END;
GO



-- END of sp_whoisactive

--/Ascent 

Use [ASCENT_DBA];
GO

IF (SELECT [Value] FROM #Config WHERE Name = 'CreateJobs') = 'Y' AND SERVERPROPERTY('EngineEdition') NOT IN(4, 5) AND (IS_SRVROLEMEMBER('sysadmin') = 1 OR (DB_ID('rdsadmin') IS NOT NULL AND SUSER_SNAME(0x01) = 'rdsa')) AND (SELECT [compatibility_level] FROM sys.databases WHERE database_id = DB_ID()) >= 90
BEGIN

  DECLARE @BackupDirectory nvarchar(max)
  DECLARE @CleanupTime int
  DECLARE @OutputFileDirectory nvarchar(max)
  DECLARE @LogToTable nvarchar(max)
  DECLARE @DatabaseName nvarchar(max)

  DECLARE @HostPlatform nvarchar(max)
  DECLARE @DirectorySeparator nvarchar(max)
  DECLARE @LogDirectory nvarchar(max)

  DECLARE @TokenServer nvarchar(max)
  DECLARE @TokenJobID nvarchar(max)
  DECLARE @TokenJobName nvarchar(max)
  DECLARE @TokenStepID nvarchar(max)
  DECLARE @TokenStepName nvarchar(max)
  DECLARE @TokenDate nvarchar(max)
  DECLARE @TokenTime nvarchar(max)
  DECLARE @TokenLogDirectory nvarchar(max)

  DECLARE @JobDescription nvarchar(max)
  DECLARE @JobCategory nvarchar(max)
  DECLARE @JobOwner nvarchar(max)

  DECLARE @Jobs TABLE (JobID int IDENTITY,
                       [Name] nvarchar(max),
                       CommandTSQL nvarchar(max),
                       CommandCmdExec nvarchar(max),
                       DatabaseName varchar(max),
                       OutputFileNamePart01 nvarchar(max),
                       OutputFileNamePart02 nvarchar(max),
                       Selected bit DEFAULT 0,
                       Completed bit DEFAULT 0)

  DECLARE @CurrentJobID int
  DECLARE @CurrentJobName nvarchar(max)
  DECLARE @CurrentCommandTSQL nvarchar(max)
  DECLARE @CurrentCommandCmdExec nvarchar(max)
  DECLARE @CurrentDatabaseName nvarchar(max)
  DECLARE @CurrentOutputFileNamePart01 nvarchar(max)
  DECLARE @CurrentOutputFileNamePart02 nvarchar(max)

  DECLARE @CurrentJobStepCommand nvarchar(max)
  DECLARE @CurrentJobStepSubSystem nvarchar(max)
  DECLARE @CurrentJobStepDatabaseName nvarchar(max)
  DECLARE @CurrentOutputFileName nvarchar(max)

  --Ascent
  DECLARE @ExistingJobStepID int

  DECLARE @CurrentJobScheduleCommand nvarchar(max)

  DECLARE @JobSchedules TABLE (ScheduleID int IDENTITY,
		jobname nvarchar(128),
		name Nvarchar(128), 
		freq_type int, 
		freq_interval int, 
		freq_subday_type int, 
		freq_subday_interval int, 
		freq_relative_interval int, 
		freq_recurrence_factor int, 
		active_start_date int, 
		active_end_date int, 
		active_start_time int, 
		active_end_time int)

  DECLARE @active_start_date int,
		@ScheduleName Nvarchar(128), 
		@Schedulefreq_type int, 
		@Schedulefreq_interval int, 
		@Schedulefreq_subday_type int, 
		@Schedulefreq_subday_interval int, 
		@Schedulefreq_relative_interval int, 
		@Schedulefreq_recurrence_factor int, 
		@Scheduleactive_start_date int, 
		@Scheduleactive_end_date int, 
		@Scheduleactive_start_time int, 
		@Scheduleactive_end_time int

  SET @active_start_date = (SELECT FORMAT( GETDATE(), 'yyyyMMdd', 'en-US' ) AS 'DateTime Result')
 --/Ascent 


  DECLARE @Version numeric(18,10) = CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max)),CHARINDEX('.',CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max))) - 1) + '.' + REPLACE(RIGHT(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max)), LEN(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max))) - CHARINDEX('.',CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max)))),'.','') AS numeric(18,10))

  DECLARE @AmazonRDS bit = CASE WHEN DB_ID('rdsadmin') IS NOT NULL AND SUSER_SNAME(0x01) = 'rdsa' THEN 1 ELSE 0 END

  IF @Version >= 14
  BEGIN
    SELECT @HostPlatform = host_platform
    FROM sys.dm_os_host_info
  END
  ELSE
  BEGIN
    SET @HostPlatform = 'Windows'
  END

  SELECT @DirectorySeparator = CASE
  WHEN @HostPlatform = 'Windows' THEN '\'
  WHEN @HostPlatform = 'Linux' THEN '/'
  END

  SET @TokenServer = '$' + '(ESCAPE_SQUOTE(SRVR))'
  SET @TokenJobID = '$' + '(ESCAPE_SQUOTE(JOBID))'
  SET @TokenStepID = '$' + '(ESCAPE_SQUOTE(STEPID))'
  SET @TokenDate = '$' + '(ESCAPE_SQUOTE(DATE))'
  SET @TokenTime = '$' + '(ESCAPE_SQUOTE(TIME))'

  IF @Version >= 13
  BEGIN
    SET @TokenJobName = '$' + '(ESCAPE_SQUOTE(JOBNAME))'
    SET @TokenStepName = '$' + '(ESCAPE_SQUOTE(STEPNAME))'
  END

  IF @Version >= 12 AND @HostPlatform = 'Windows'
  BEGIN
    SET @TokenLogDirectory = '$' + '(ESCAPE_SQUOTE(SQLLOGDIR))'
  END

  SELECT @BackupDirectory = Value
  FROM #Config
  WHERE [Name] = 'BackupDirectory'

  IF @HostPlatform = 'Windows'
  BEGIN
    SELECT @CleanupTime = Value
    FROM #Config
    WHERE [Name] = 'CleanupTime'
  END

  SELECT @OutputFileDirectory = Value
  FROM #Config
  WHERE [Name] = 'OutputFileDirectory'

  SELECT @LogToTable = Value
  FROM #Config
  WHERE [Name] = 'LogToTable'

  SELECT @DatabaseName = Value
  FROM #Config
  WHERE [Name] = 'DatabaseName'

  IF @Version >= 11
  BEGIN
    SELECT @LogDirectory = [path]
    FROM sys.dm_os_server_diagnostics_log_configurations
  END
  ELSE
  BEGIN
    SELECT @LogDirectory = LEFT(CAST(SERVERPROPERTY('ErrorLogFileName') AS nvarchar(max)),LEN(CAST(SERVERPROPERTY('ErrorLogFileName') AS nvarchar(max))) - CHARINDEX('\',REVERSE(CAST(SERVERPROPERTY('ErrorLogFileName') AS nvarchar(max)))))
  END

  IF @OutputFileDirectory IS NOT NULL AND RIGHT(@OutputFileDirectory,1) = @DirectorySeparator
  BEGIN
    SET @OutputFileDirectory = LEFT(@OutputFileDirectory, LEN(@OutputFileDirectory) - 1)
  END

  IF @LogDirectory IS NOT NULL AND RIGHT(@LogDirectory,1) = @DirectorySeparator
  BEGIN
    SET @LogDirectory = LEFT(@LogDirectory, LEN(@LogDirectory) - 1)
  END

  SET @JobDescription = 'Database Maintenance Solution provided by 
  Ascent Technology PTY (Ltd)
  https://www.ascent.tech '
  SET @JobCategory = 'Database Maintenance'

  IF @AmazonRDS = 0
  BEGIN
    SET @JobOwner = SUSER_SNAME(0x01)
  END

  INSERT INTO @Jobs ([Name], CommandTSQL, DatabaseName, OutputFileNamePart01, OutputFileNamePart02)
  SELECT 'Ascent_DBA - Backup - SYSTEM_DATABASES - FULL',
         'EXECUTE [dbo].[DatabaseBackup]' + CHAR(13) + CHAR(10) + '@Databases = ''SYSTEM_DATABASES'',' + CHAR(13) + CHAR(10) + '@Directory = ' + ISNULL('N''' + REPLACE(@BackupDirectory,'''','''''') + '''','NULL') + ',' + CHAR(13) + CHAR(10) + '@BackupType = ''FULL'',' + CHAR(13) + CHAR(10) + '@Verify = ''Y'',' + CHAR(13) + CHAR(10) + '@CleanupTime = ' + ISNULL(CAST(@CleanupTime AS nvarchar),'NULL') + ',' + CHAR(13) + CHAR(10) + '@CheckSum = ''Y'',' + CHAR(13) + CHAR(10) + '@Compress = ''Y'',' + CHAR(13) + CHAR(10) + '@LogToTable = ''' + @LogToTable + '''',
         @DatabaseName,
         'DatabaseBackup',
         'FULL'

  INSERT INTO @Jobs ([Name], CommandTSQL, DatabaseName, OutputFileNamePart01, OutputFileNamePart02)
  SELECT 'Ascent_DBA - Backup - USER_DATABASES - DIFF',
         'EXECUTE [dbo].[DatabaseBackup]' + CHAR(13) + CHAR(10) + '@Databases = ''USER_DATABASES'',' + CHAR(13) + CHAR(10) + '@Directory = ' + ISNULL('N''' + REPLACE(@BackupDirectory,'''','''''') + '''','NULL') + ',' + CHAR(13) + CHAR(10) + '@BackupType = ''DIFF'',' + CHAR(13) + CHAR(10) + '@Verify = ''Y'',' + CHAR(13) + CHAR(10) + '@CleanupTime = ' + ISNULL(CAST(@CleanupTime AS nvarchar),'NULL') + ',' + CHAR(13) + CHAR(10) + '@CheckSum = ''Y'',' + CHAR(13) + CHAR(10) + '@Compress = ''Y'',' + CHAR(13) + CHAR(10) + '@LogToTable = ''' + @LogToTable + '''',
          @DatabaseName,
         'DatabaseBackup',
         'DIFF'

  INSERT INTO @Jobs ([Name], CommandTSQL, DatabaseName, OutputFileNamePart01, OutputFileNamePart02)
  SELECT 'Ascent_DBA - Backup - USER_DATABASES - FULL',
         'EXECUTE [dbo].[DatabaseBackup]' + CHAR(13) + CHAR(10) + '@Databases = ''USER_DATABASES'',' + CHAR(13) + CHAR(10) + '@Directory = ' + ISNULL('N''' + REPLACE(@BackupDirectory,'''','''''') + '''','NULL') + ',' + CHAR(13) + CHAR(10) + '@BackupType = ''FULL'',' + CHAR(13) + CHAR(10) + '@Verify = ''Y'',' + CHAR(13) + CHAR(10) + '@CleanupTime = ' + ISNULL(CAST(@CleanupTime AS nvarchar),'NULL') + ',' + CHAR(13) + CHAR(10) + '@CheckSum = ''Y'',' + CHAR(13) + CHAR(10) + '@Compress = ''Y'',' + CHAR(13) + CHAR(10) + '@LogToTable = ''' + @LogToTable + '''',
         @DatabaseName,
         'DatabaseBackup',
         'FULL'

  INSERT INTO @Jobs ([Name], CommandTSQL, DatabaseName, OutputFileNamePart01, OutputFileNamePart02)
  SELECT 'Ascent_DBA - Backup - USER_DATABASES - LOG',
         'EXECUTE [dbo].[DatabaseBackup]' + CHAR(13) + CHAR(10) + '@Databases = ''USER_DATABASES'',' + CHAR(13) + CHAR(10) + '@Directory = ' + ISNULL('N''' + REPLACE(@BackupDirectory,'''','''''') + '''','NULL') + ',' + CHAR(13) + CHAR(10) + '@BackupType = ''LOG'',' + CHAR(13) + CHAR(10) + '@Verify = ''Y'',' + CHAR(13) + CHAR(10) + '@CleanupTime = ' + ISNULL(CAST(@CleanupTime AS nvarchar),'NULL') + ',' + CHAR(13) + CHAR(10) + '@CheckSum = ''Y'',' + CHAR(13) + CHAR(10) + '@Compress = ''Y'',' + CHAR(13) + CHAR(10) + '@LogToTable = ''' + @LogToTable + '''',
         @DatabaseName,
         'DatabaseBackup',
         'LOG'

  INSERT INTO @Jobs ([Name], CommandTSQL, DatabaseName, OutputFileNamePart01)
  SELECT 'Ascent_DBA - Integrity_Check - SYSTEM_DATABASES',
         'EXECUTE [dbo].[DatabaseIntegrityCheck]' + CHAR(13) + CHAR(10) + '@Databases = ''SYSTEM_DATABASES'',' + CHAR(13) + CHAR(10) + '@LogToTable = ''' + @LogToTable + '''',
         @DatabaseName,
         'DatabaseIntegrityCheck'

  INSERT INTO @Jobs ([Name], CommandTSQL, DatabaseName, OutputFileNamePart01)
  SELECT 'Ascent_DBA - Integrity_Check - USER_DATABASES',
         'EXECUTE [dbo].[DatabaseIntegrityCheck]' + CHAR(13) + CHAR(10) + '@Databases = ''USER_DATABASES' + CASE WHEN @AmazonRDS = 1 THEN ', -rdsadmin' ELSE '' END + ''',' + CHAR(13) + CHAR(10) + '@LogToTable = ''' + @LogToTable + '''',
         @DatabaseName,
         'DatabaseIntegrityCheck'

  INSERT INTO @Jobs ([Name], CommandTSQL, DatabaseName, OutputFileNamePart01)
  SELECT 'Ascent_DBA - Index_Optimization - USER_DATABASES',
         'EXECUTE [dbo].[IndexOptimize]' + CHAR(13) + CHAR(10) + '@Databases = ''USER_DATABASES'',' + CHAR(13) + CHAR(10) + '@LogToTable = ''' + @LogToTable + '''',
         @DatabaseName,
         'IndexOptimize'

  INSERT INTO @Jobs ([Name], CommandTSQL, DatabaseName, OutputFileNamePart01)
SELECT 'Ascent_DBA - Statistics_Optimization - USER_DATABASES',
         'EXECUTE [dbo].[IndexOptimize]' + CHAR(13) + CHAR(10) + '@Databases = ''USER_DATABASES'',' + CHAR(13) + CHAR(10) + '@FragmentationLow = NULL ,' + CHAR(13) + CHAR(10) + '@FragmentationMedium = NULL ,' + CHAR(13) + CHAR(10) + '@FragmentationHigh = NULL ,' + CHAR(13) + CHAR(10) + '@UpdateStatistics = ''ALL'',' + CHAR(13) + CHAR(10) + '@OnlyModifiedStatistics = N''Y'',' + CHAR(13) + CHAR(10) + '@LogToTable = ''' + @LogToTable + '''',
         @DatabaseName,
         'StatsOptimize'
		 
  INSERT INTO @Jobs ([Name], CommandTSQL, DatabaseName, OutputFileNamePart01)																 
  SELECT 'Ascent_DBA - Delete_Backup_History',
         'DECLARE @CleanupDate datetime' + CHAR(13) + CHAR(10) + 'SET @CleanupDate = DATEADD(dd,-30,GETDATE())' + CHAR(13) + CHAR(10) + 'EXECUTE dbo.sp_delete_backuphistory @oldest_date = @CleanupDate',
         'msdb',
         'sp_delete_backuphistory'

  INSERT INTO @Jobs ([Name], CommandTSQL, DatabaseName, OutputFileNamePart01)
  SELECT 'Ascent_DBA - Purge_Job_History',
         'DECLARE @CleanupDate datetime' + CHAR(13) + CHAR(10) + 'SET @CleanupDate = DATEADD(dd,-30,GETDATE())' + CHAR(13) + CHAR(10) + 'EXECUTE dbo.sp_purge_jobhistory @oldest_date = @CleanupDate',
         'msdb',
         'sp_purge_jobhistory'

  INSERT INTO @Jobs ([Name], CommandTSQL, DatabaseName, OutputFileNamePart01)
  SELECT 'Ascent_DBA - Cleanup_CommandLog',
         'DELETE FROM [dbo].[CommandLog]' + CHAR(13) + CHAR(10) + 'WHERE StartTime < DATEADD(dd,-30,GETDATE())',
         @DatabaseName,
         'CommandLogCleanup'

  INSERT INTO @Jobs ([Name], CommandCmdExec, OutputFileNamePart01)
  SELECT 'Ascent_DBA - Cleanup_Output_Files',
         'cmd /q /c "For /F "tokens=1 delims=" %v In (''ForFiles /P "' + COALESCE(@OutputFileDirectory,@TokenLogDirectory,@LogDirectory) + '" /m *_*_*_*.txt /d -30 2^>^&1'') do if EXIST "' + COALESCE(@OutputFileDirectory,@TokenLogDirectory,@LogDirectory) + '"\%v echo del "' + COALESCE(@OutputFileDirectory,@TokenLogDirectory,@LogDirectory) + '"\%v& del "' + COALESCE(@OutputFileDirectory,@TokenLogDirectory,@LogDirectory) + '"\%v"',
         'OutputFileCleanup'

  --Ascent
  INSERT INTO @JobSchedules ([jobname], [Name], [freq_type], [freq_interval], [freq_subday_type], [freq_subday_interval], [freq_relative_interval], [freq_recurrence_factor], [active_start_date], [active_end_date], [active_start_time], [active_end_time])
  SELECT 'Ascent_DBA - Integrity_Check - USER_DATABASES', 
		'Sun@3AM', 
		--@enabled=1, 
		'8', 
		'1', 
		'1', 
		'0', 
		'0', 
		'1', 
		@active_start_date, 
		'99991231', 
		'30000', 
		'235959'

  INSERT INTO @JobSchedules ([jobname], [Name], [freq_type], [freq_interval], [freq_subday_type], [freq_subday_interval], [freq_relative_interval], [freq_recurrence_factor], [active_start_date], [active_end_date], [active_start_time], [active_end_time])
  SELECT 'Ascent_DBA - Backup - SYSTEM_DATABASES - FULL',
        'Daily@10PM', 
		--1, 
		4, 
		1, 
		1, 
		0, 
		0, 
		1, 
		@active_start_date, 
		99991231, 
		220000, 
		235959

  INSERT INTO @JobSchedules ([jobname], [Name], [freq_type], [freq_interval], [freq_subday_type], [freq_subday_interval], [freq_relative_interval], [freq_recurrence_factor], [active_start_date], [active_end_date], [active_start_time], [active_end_time])
  SELECT 'Ascent_DBA - Backup - USER_DATABASES - FULL',
        'Daily@10PM', 
		--1, 
		4, 
		1, 
		1, 
		0, 
		0, 
		1, 
		@active_start_date, 
		99991231, 
		220000, 
		235959

  INSERT INTO @JobSchedules ([jobname], [Name], [freq_type], [freq_interval], [freq_subday_type], [freq_subday_interval], [freq_relative_interval], [freq_recurrence_factor], [active_start_date], [active_end_date], [active_start_time], [active_end_time])
  SELECT 'Ascent_DBA - Backup - USER_DATABASES - LOG',
        'DailyEvery1Hours', 
		--1, 
		4, 
		1, 
		8, 
		1, 
		0, 
		1, 
		@active_start_date, 
		99991231, 
		0, 
		235959

  INSERT INTO @JobSchedules ([jobname], [Name], [freq_type], [freq_interval], [freq_subday_type], [freq_subday_interval], [freq_relative_interval], [freq_recurrence_factor], [active_start_date], [active_end_date], [active_start_time], [active_end_time])
  SELECT 'Ascent_DBA - Integrity_Check - SYSTEM_DATABASES',
        'Sun@2AM', 
		--@enabled=1, 
		8, 
		1, 
		1, 
		0, 
		0, 
		1, 
		@active_start_date, 
		99991231, 
		20000, 
		235959

  INSERT INTO @JobSchedules ([jobname], [Name], [freq_type], [freq_interval], [freq_subday_type], [freq_subday_interval], [freq_relative_interval], [freq_recurrence_factor], [active_start_date], [active_end_date], [active_start_time], [active_end_time])
  SELECT 'Ascent_DBA - Integrity_Check - USER_DATABASES',
         'Sun@2AM', 
		--@enabled=1, 
		8, 
		1, 
		1, 
		0, 
		0, 
		1, 
		@active_start_date, 
		99991231, 
		20000, 
		235959

  INSERT INTO @JobSchedules ([jobname], [Name], [freq_type], [freq_interval], [freq_subday_type], [freq_subday_interval], [freq_relative_interval], [freq_recurrence_factor], [active_start_date], [active_end_date], [active_start_time], [active_end_time])
  SELECT 'Ascent_DBA - Index_Optimization - USER_DATABASES',
        'Sat@2AM', 
		--@enabled=1, 
		8, 
		64, 
		1, 
		0, 
		0, 
		1, 
		@active_start_date, 
		99991231, 
		20000, 
		235959

  INSERT INTO @JobSchedules ([jobname], [Name], [freq_type], [freq_interval], [freq_subday_type], [freq_subday_interval], [freq_relative_interval], [freq_recurrence_factor], [active_start_date], [active_end_date], [active_start_time], [active_end_time])
  SELECT 'Ascent_DBA - Statistics_Optimization - USER_DATABASES',
        'Daily@9PM', 
		--@enabled=1, 
		4, 
		1, 
		1, 
		0, 
		0, 
		1, 
		@active_start_date, 
		99991231, 
		210000, 
		235959
		 
  INSERT INTO @JobSchedules ([jobname], [Name], [freq_type], [freq_interval], [freq_subday_type], [freq_subday_interval], [freq_relative_interval], [freq_recurrence_factor], [active_start_date], [active_end_date], [active_start_time], [active_end_time])
  SELECT 'Ascent_DBA - Delete_Backup_History',
         'LastDayEvry3Mnths@1100PM', 
		--@enabled=1, 
		32, 
		8, 
		1, 
		0, 
		16, 
		3, 
		@active_start_date, 
		99991231, 
		230000, 
		235959

  INSERT INTO @JobSchedules ([jobname], [Name], [freq_type], [freq_interval], [freq_subday_type], [freq_subday_interval], [freq_relative_interval], [freq_recurrence_factor], [active_start_date], [active_end_date], [active_start_time], [active_end_time])
  SELECT 'Ascent_DBA - Purge_Job_History',
        'LastDayEvry3Mnths@1100PM', 
		--@enabled=1, 
		32, 
		8, 
		1, 
		0, 
		16, 
		3, 
		@active_start_date, 
		99991231, 
		230000, 
		235959

  INSERT INTO @JobSchedules ([jobname], [Name], [freq_type], [freq_interval], [freq_subday_type], [freq_subday_interval], [freq_relative_interval], [freq_recurrence_factor], [active_start_date], [active_end_date], [active_start_time], [active_end_time])
  SELECT 'Ascent_DBA - Cleanup_CommandLog',
        'LastDayEvry3Mnths@1130PM', 
		--1, 
		32, 
		8, 
		1, 
		0, 
		16, 
		3, 
		@active_start_date, 
		99991231, 
		233000, 
		235959

  INSERT INTO @JobSchedules ([jobname], [Name], [freq_type], [freq_interval], [freq_subday_type], [freq_subday_interval], [freq_relative_interval], [freq_recurrence_factor], [active_start_date], [active_end_date], [active_start_time], [active_end_time])
  SELECT 'Ascent_DBA - Cleanup_Output_Files',
        'LastDayEvry1Mnth@1100PM', 
		--@enabled=1, 
		32, 
		8, 
		1, 
		0, 
		16, 
		1, 
		@active_start_date, 
		99991231, 
		230000, 
		235959

	SELECT * FROM @Jobs
	SELECT * FROM @JobSchedules
--/Ascent


  IF @AmazonRDS = 1
  BEGIN
   UPDATE @Jobs
   SET Selected = 1
   WHERE [Name] IN('DatabaseIntegrityCheck - USER_DATABASES','IndexOptimize - USER_DATABASES','CommandLog Cleanup')
  END
  ELSE IF SERVERPROPERTY('EngineEdition') = 8
  BEGIN
   UPDATE @Jobs
   SET Selected = 1
   WHERE [Name] IN('DatabaseIntegrityCheck - SYSTEM_DATABASES','DatabaseIntegrityCheck - USER_DATABASES','IndexOptimize - USER_DATABASES','CommandLog Cleanup','sp_delete_backuphistory','sp_purge_jobhistory')
  END
  ELSE IF @HostPlatform = 'Windows'
  BEGIN
   UPDATE @Jobs
   SET Selected = 1
  END
  ELSE IF @HostPlatform = 'Linux'
  BEGIN
   UPDATE @Jobs
   SET Selected = 1
   WHERE CommandTSQL IS NOT NULL
  END

 -- Ascent
  IF (SELECT [Value] FROM #Config WHERE Name = 'CreateBackupJobs') <> 'Y'
  BEGIN
   UPDATE @Jobs
   SET Selected = 0
   WHERE [Name] IN('Ascent_DBA - Backup - USER_DATABASES - LOG','Ascent_DBA - Backup - USER_DATABASES - FULL','Ascent_DBA - Backup - USER_DATABASES - DIFF','Ascent_DBA - Delete_Backup_History','Ascent_DBA - Backup - SYSTEM_DATABASES - FULL')
  END
  
  IF (SELECT [Value] FROM #Config WHERE Name = 'CreateIndexJobs') <> 'Y' 
  BEGIN
   UPDATE @Jobs
   SET Selected = 0
   WHERE [Name] IN('Ascent_DBA - Index_Optimization - USER_DATABASES','Ascent_DBA - Statistics_Optimization - USER_DATABASES')
  END
  
  IF (SELECT [Value] FROM #Config WHERE Name = 'CreateDBCCJobs') <> 'Y' 
  BEGIN
   UPDATE @Jobs
   SET Selected = 0
   WHERE [Name] IN('Ascent_DBA - Integrity_Check - SYSTEM_DATABASES','Ascent_DBA - Integrity_Check - USER_DATABASES')
  END

  --  /Ascent
 

  WHILE EXISTS (SELECT * FROM @Jobs WHERE Completed = 0 AND Selected = 1)
  BEGIN
    SELECT @CurrentJobID = JobID,
           @CurrentJobName = [Name],
           @CurrentCommandTSQL = CommandTSQL,
           @CurrentCommandCmdExec = CommandCmdExec,
           @CurrentDatabaseName = DatabaseName,
           @CurrentOutputFileNamePart01 = OutputFileNamePart01,
           @CurrentOutputFileNamePart02 = OutputFileNamePart02
    FROM @Jobs
    WHERE Completed = 0
    AND Selected = 1
    ORDER BY JobID ASC

    IF @CurrentCommandTSQL IS NOT NULL AND @AmazonRDS = 1
    BEGIN
      SET @CurrentJobStepSubSystem = 'TSQL'
      SET @CurrentJobStepCommand = @CurrentCommandTSQL
      SET @CurrentJobStepDatabaseName = @CurrentDatabaseName
    END
    ELSE IF @CurrentCommandTSQL IS NOT NULL AND SERVERPROPERTY('EngineEdition') = 8
    BEGIN
      SET @CurrentJobStepSubSystem = 'TSQL'
      SET @CurrentJobStepCommand = @CurrentCommandTSQL
      SET @CurrentJobStepDatabaseName = @CurrentDatabaseName
    END
    ELSE IF @CurrentCommandTSQL IS NOT NULL AND @HostPlatform = 'Linux'
    BEGIN
      SET @CurrentJobStepSubSystem = 'TSQL'
      SET @CurrentJobStepCommand = @CurrentCommandTSQL
      SET @CurrentJobStepDatabaseName = @CurrentDatabaseName
    END
    ELSE IF @CurrentCommandTSQL IS NOT NULL AND @HostPlatform = 'Windows' AND @Version >= 11
    BEGIN
      SET @CurrentJobStepSubSystem = 'TSQL'
      SET @CurrentJobStepCommand = @CurrentCommandTSQL
      SET @CurrentJobStepDatabaseName = @CurrentDatabaseName
    END
    ELSE IF @CurrentCommandTSQL IS NOT NULL AND @HostPlatform = 'Windows' AND @Version < 11
    BEGIN
      SET @CurrentJobStepSubSystem = 'CMDEXEC'
      SET @CurrentJobStepCommand = 'sqlcmd -E -S ' + @TokenServer + ' -d ' + @CurrentDatabaseName + ' -Q "' + REPLACE(@CurrentCommandTSQL,(CHAR(13) + CHAR(10)),' ') + '" -b'
      SET @CurrentJobStepDatabaseName = NULL
    END
    ELSE IF @CurrentCommandCmdExec IS NOT NULL AND @HostPlatform = 'Windows'
    BEGIN
      SET @CurrentJobStepSubSystem = 'CMDEXEC'
      SET @CurrentJobStepCommand = @CurrentCommandCmdExec
      SET @CurrentJobStepDatabaseName = NULL
    END

    IF @AmazonRDS = 0 AND SERVERPROPERTY('EngineEdition') <> 8
    BEGIN
      SET @CurrentOutputFileName = COALESCE(@OutputFileDirectory,@TokenLogDirectory,@LogDirectory) + @DirectorySeparator + ISNULL(CASE WHEN @TokenJobName IS NULL THEN @CurrentOutputFileNamePart01 END + '_','') + ISNULL(CASE WHEN @TokenJobName IS NULL THEN @CurrentOutputFileNamePart02 END + '_','') + ISNULL(@TokenJobName,@TokenJobID) + '_' + @TokenStepID + '_' + @TokenDate + '_' + @TokenTime + '.txt'
      IF LEN(@CurrentOutputFileName) > 200 SET @CurrentOutputFileName = COALESCE(@OutputFileDirectory,@TokenLogDirectory,@LogDirectory) + @DirectorySeparator + ISNULL(CASE WHEN @TokenJobName IS NULL THEN @CurrentOutputFileNamePart01 END + '_','') + ISNULL(@TokenJobName,@TokenJobID) + '_' + @TokenStepID + '_' + @TokenDate + '_' + @TokenTime + '.txt'
      IF LEN(@CurrentOutputFileName) > 200 SET @CurrentOutputFileName = COALESCE(@OutputFileDirectory,@TokenLogDirectory,@LogDirectory) + @DirectorySeparator + ISNULL(@TokenJobName,@TokenJobID) + '_' + @TokenStepID + '_' + @TokenDate + '_' + @TokenTime + '.txt'
      IF LEN(@CurrentOutputFileName) > 200 SET @CurrentOutputFileName = NULL
    END

	IF @CurrentJobStepSubSystem IS NOT NULL AND @CurrentJobStepCommand IS NOT NULL AND EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = @CurrentJobName)
    BEGIN
	  IF (SELECT [Value] FROM #Config WHERE Name = 'ExistingJobsAction')
	  = 'REPLACE'  
	    BEGIN 
	      EXECUTE msdb.dbo.sp_delete_jobstep @job_name = @CurrentJobName, @step_id = 0
		  EXECUTE msdb.dbo.sp_delete_job @job_name = @CurrentJobName, @delete_history = 0, @delete_unused_schedule = 1
	    END
	  ELSE IF (SELECT [Value] FROM #Config WHERE Name = 'ExistingJobsAction')
	  =  'UPDATE' 
	    BEGIN
		  EXECUTE msdb.dbo.sp_update_job @job_name = @CurrentJobName, @description = @JobDescription, @category_name = @JobCategory, @owner_login_name = @JobOwner
      
	      IF EXISTS (SELECT * FROM msdb.dbo.sysjobsteps sjs INNER JOIN msdb.dbo.sysjobs sj ON sj.job_id = sjs.job_id WHERE sj.name = @CurrentJobName AND sjs.step_name = @CurrentJobName)
	      BEGIN
	        SELECT @ExistingJobStepID = sjs.step_id FROM msdb.dbo.sysjobsteps sjs INNER JOIN msdb.dbo.sysjobs sj ON sj.job_id = sjs.job_id WHERE sj.name = @CurrentJobName AND sjs.step_name = @CurrentJobName;
	        EXECUTE msdb.dbo.sp_update_jobstep @job_name = @CurrentJobName, @step_id = @ExistingJobStepID, @step_name = @CurrentJobName, @subsystem = @CurrentJobStepSubSystem, @command = @CurrentJobStepCommand, @output_file_name = @CurrentOutputFileName, @database_name = @CurrentJobStepDatabaseName
	      END
		END
		END


    IF @CurrentJobStepSubSystem IS NOT NULL AND @CurrentJobStepCommand IS NOT NULL AND NOT EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = @CurrentJobName)
    BEGIN
      EXECUTE msdb.dbo.sp_add_job @job_name = @CurrentJobName, @description = @JobDescription, @category_name = @JobCategory, @owner_login_name = @JobOwner
      EXECUTE msdb.dbo.sp_add_jobstep @job_name = @CurrentJobName, @step_name = @CurrentJobName, @subsystem = @CurrentJobStepSubSystem, @command = @CurrentJobStepCommand, @output_file_name = @CurrentOutputFileName, @database_name = @CurrentJobStepDatabaseName
      EXECUTE msdb.dbo.sp_add_jobserver @job_name = @CurrentJobName
    END
	ELSE
	BEGIN
	  EXECUTE msdb.dbo.sp_update_job @job_name = @CurrentJobName, @description = @JobDescription, @category_name = @JobCategory, @owner_login_name = @JobOwner
      
	  IF EXISTS (SELECT * FROM msdb.dbo.sysjobsteps sjs INNER JOIN msdb.dbo.sysjobs sj ON sj.job_id = sjs.job_id WHERE sj.name = @CurrentJobName AND sjs.step_name = @CurrentJobName)
	  BEGIN
	    SELECT @ExistingJobStepID = sjs.step_id FROM msdb.dbo.sysjobsteps sjs INNER JOIN msdb.dbo.sysjobs sj ON sj.job_id = sjs.job_id WHERE sj.name = @CurrentJobName AND sjs.step_name = @CurrentJobName;
	    EXECUTE msdb.dbo.sp_update_jobstep @job_name = @CurrentJobName, @step_id = @ExistingJobStepID, @step_name = @CurrentJobName, @subsystem = @CurrentJobStepSubSystem, @command = @CurrentJobStepCommand, @output_file_name = @CurrentOutputFileName, @database_name = @CurrentJobStepDatabaseName
	  END
     
	END

	IF (SELECT [Value] FROM #Config WHERE Name = 'CreateJobSchedules') = 'Y'
	BEGIN
	  
	  SELECT @ScheduleName = NAME,
	    @Schedulefreq_type = freq_type,
	    @Schedulefreq_interval = freq_interval,
	    @Schedulefreq_subday_type = freq_subday_type, 
	    @Schedulefreq_subday_interval = freq_subday_interval, 
	    @Schedulefreq_relative_interval = freq_relative_interval, 
		@Schedulefreq_recurrence_factor = freq_recurrence_factor, 
		@Scheduleactive_start_date = active_start_date, 
		@Scheduleactive_end_date = active_end_date, 
		@Scheduleactive_start_time = active_start_time, 
		@Scheduleactive_end_time = active_end_time
	  FROM @JobSchedules
	  WHERE jobname = @CurrentJobName
	  
	  IF @ScheduleName IS NOT NULL
	  BEGIN
	  EXECUTE msdb.dbo.sp_add_jobschedule @job_name=@CurrentJobName,
	    @name = @ScheduleName, 
		@enabled=1, 
		@freq_type = @Schedulefreq_type, 
		@freq_interval = @Schedulefreq_interval,
		@freq_subday_type = @Schedulefreq_subday_type,
		@freq_subday_interval = @Schedulefreq_subday_interval,
		@freq_relative_interval = @Schedulefreq_relative_interval,
		@freq_recurrence_factor = @Schedulefreq_recurrence_factor,
		@active_start_date = @Scheduleactive_start_date, 
		@active_end_date = @Scheduleactive_end_date,
		@active_start_time = @Scheduleactive_start_time,
		@active_end_time = @Scheduleactive_end_time
	  END
	END


    UPDATE Jobs
    SET Completed = 1
    FROM @Jobs Jobs
    WHERE JobID = @CurrentJobID

    SET @CurrentJobID = NULL
    SET @CurrentJobName = NULL
    SET @CurrentCommandTSQL = NULL
    SET @CurrentCommandCmdExec = NULL
    SET @CurrentDatabaseName = NULL
    SET @CurrentOutputFileNamePart01 = NULL
    SET @CurrentOutputFileNamePart02 = NULL
    SET @CurrentJobStepCommand = NULL
    SET @CurrentJobStepSubSystem = NULL
    SET @CurrentJobStepDatabaseName = NULL
    SET @CurrentOutputFileName = NULL

	SET @ScheduleName = NULL
	SET @Schedulefreq_type = NULL
	SET @Schedulefreq_interval = NULL
	SET @Schedulefreq_subday_type = NULL
	SET @Schedulefreq_subday_interval = NULL
	SET @Schedulefreq_relative_interval = NULL
	SET @Schedulefreq_recurrence_factor = NULL
	SET @Scheduleactive_start_date = NULL
	SET @Scheduleactive_end_date = NULL
	SET @Scheduleactive_start_time = NULL
	SET @Scheduleactive_end_time = NULL


  END

END
GO


---------------------------------------------------------------------------------------------------------------------------------------
-- Uninstall Objects and jobs
---------------------------------------------------------------------------------------------------------------------------------------

/*


IF OBJECT_ID('[dbo].[DatabaseBackup]') IS NOT NULL DROP PROCEDURE [dbo].[DatabaseBackup]
IF OBJECT_ID('[dbo].[DatabaseIntegrityCheck]') IS NOT NULL DROP PROCEDURE [dbo].[DatabaseIntegrityCheck]
IF OBJECT_ID('[dbo].[IndexOptimize]') IS NOT NULL DROP PROCEDURE [dbo].[IndexOptimize]
IF OBJECT_ID('[dbo].[CommandExecute]') IS NOT NULL DROP PROCEDURE [dbo].[CommandExecute]
IF OBJECT_ID('[dbo].[DatabaseSelect]') IS NOT NULL DROP FUNCTION [dbo].[DatabaseSelect]
IF OBJECT_ID('[dbo].[CommandLog]') IS NOT NULL DROP TABLE [dbo].[CommandLog]
IF OBJECT_ID('[dbo].[DatabaseRestore]') IS NOT NULL DROP PROCEDURE [dbo].[DatabaseRestore]


USE [msdb];
*/
/*
SELECT 'IF EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = '''+name+''') EXECUTE msdb.dbo.sp_delete_job @job_name = '''+name+''', @delete_unused_schedule=1' FROM msdb.dbo.sysjobs WHERE [name] like 'Ascent_DBA - %'
*/
/*
IF EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = 'Ascent_DBA - Backup - SYSTEM_DATABASES - FULL') EXECUTE msdb.dbo.sp_delete_job @job_name = 'Ascent_DBA - Backup - SYSTEM_DATABASES - FULL', @delete_unused_schedule=1
IF EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = 'Ascent_DBA - Backup - USER_DATABASES - DIFF') EXECUTE msdb.dbo.sp_delete_job @job_name = 'Ascent_DBA - Backup - USER_DATABASES - DIFF', @delete_unused_schedule=1
IF EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = 'Ascent_DBA - Backup - USER_DATABASES - FULL') EXECUTE msdb.dbo.sp_delete_job @job_name = 'Ascent_DBA - Backup - USER_DATABASES - FULL', @delete_unused_schedule=1
IF EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = 'Ascent_DBA - Backup - USER_DATABASES - LOG') EXECUTE msdb.dbo.sp_delete_job @job_name = 'Ascent_DBA - Backup - USER_DATABASES - LOG', @delete_unused_schedule=1

IF EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = 'Ascent_DBA - Cleanup_CommandLog') EXECUTE msdb.dbo.sp_delete_job @job_name = 'Ascent_DBA - Cleanup_CommandLog', @delete_unused_schedule=1
IF EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = 'Ascent_DBA - Cleanup_Output_Files') EXECUTE msdb.dbo.sp_delete_job @job_name = 'Ascent_DBA - Cleanup_Output_Files', @delete_unused_schedule=1

IF EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = 'Ascent_DBA - Delete_Backup_History') EXECUTE msdb.dbo.sp_delete_job @job_name = 'Ascent_DBA - Delete_Backup_History', @delete_unused_schedule=1
IF EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = 'Ascent_DBA - Index_Optimization - USER_DATABASES') EXECUTE msdb.dbo.sp_delete_job @job_name = 'Ascent_DBA - Index_Optimization - USER_DATABASES', @delete_unused_schedule=1
IF EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = 'Ascent_DBA - Integrity_Check - SYSTEM_DATABASES') EXECUTE msdb.dbo.sp_delete_job @job_name = 'Ascent_DBA - Integrity_Check - SYSTEM_DATABASES', @delete_unused_schedule=1
IF EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = 'Ascent_DBA - Integrity_Check - USER_DATABASES') EXECUTE msdb.dbo.sp_delete_job @job_name = 'Ascent_DBA - Integrity_Check - USER_DATABASES', @delete_unused_schedule=1

IF EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = 'Ascent_DBA - Purge_Job_History') EXECUTE msdb.dbo.sp_delete_job @job_name = 'Ascent_DBA - Purge_Job_History', @delete_unused_schedule=1
IF EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = 'Ascent_DBA - Statistics_Optimization - USER_DATABASES') EXECUTE msdb.dbo.sp_delete_job @job_name = 'Ascent_DBA - Statistics_Optimization - USER_DATABASES', @delete_unused_schedule=1

*/
