
/****** Object:  StoredProcedure [dbo].[usp_ReplicationReport]    Script Date: 4/24/2024 9:30:59 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		Keith Mac Lure
-- Create date: 23-03-2022
-- Description:	Send Replication Report to Recipient
-- Usage: EXECUTE dbo.usp_ReplicationReport @Distributer, @CompanyName, @Mailto
-- Example: EXECUTE dbo.usp_ReplicationReport @Distributer = 'SQLServer', @CompanyName = 'Data Group', @Mailto = 'keith.maclure.ext@gmail.com'
-- =============================================
CREATE PROCEDURE [dbo].[usp_ReplicationReport] 
@Distributer SYSNAME,
@CompanyName VARCHAR(50),
@Mailto VARCHAR(500)

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

DECLARE  @TSQL NVARCHAR(MAX)
		,@HTML NVARCHAR(MAX)
		,@FullHTML NVARCHAR(MAX);

/*		
-- For Testing
DECLARE @Distributer SYSNAME,
@CompanyName VARCHAR(50),
@Mailto VARCHAR(500)

SET @Distributer = 'SQLServer'
SET @CompanyName = 'Data Group'
SET @Mailto = 'Keith.MacLure.ext@gmail.com'
*/

------------------------------------
--Create Style Sheet for HTML mail--
------------------------------------
SET @FullHTML = N'<style type="text/css">
	#box-table
	{
	font-family: "Calibri", "Candara", Sans-Serif;
	text-align: Left;
	border-collapse: collapse;
	border-top: 7px solid #6eebac;
	font-size: 12px;
	font-weight: normal;
	}
	#box-table th
	{
	font-size: 15px;
	font-weight: normal;
	background: #b1f4da;
	padding-left: 6px;
	padding-right: 6px;
	border-right: 1px solid #969696;
	border-left: 1px solid #969696;
	border-bottom: 1px solid #969696;
	color: #000000;
	}
	#box-table td
	{
	padding-left: 6px;
	padding-right: 6px;
	border-right: 1px solid #969696;
	border-left: 1px solid #969696;
	border-bottom: 1px solid #969696;
	color: #000000;
	}
	#box-table tr:nth-child(odd) { background-color:#eee; }
	tr:nth-child(even) { background-color:#fff; } 
	H3 { 
	display: block;
	font-size: 20px;
	margin-top: 0px;
	margin-bottom: 0px;
	margin-left: 0;
	margin-right: 0;
	font-weight: bold;
	font-family: "Calibri";
	}
	H2 { 
	display: block;
	font-size: 16px;
	margin-top: 0px;
	margin-bottom: 0px;
	margin-left: 0;
	margin-right: 0;
	font-weight: normal;
	font-family: "Calibri";
	}
	</style>'



IF OBJECT_ID('tempdb..#ReplicationStatus') IS NOT NULL
	DROP TABLE #ReplicationStatus

CREATE TABLE #ReplicationStatus (
	[InstanceName] [nvarchar](128) NULL,
	[Publisher] [sysname] NULL,
	[Subscriber] [nvarchar](128) NULL,
	[FriendlyName] [nvarchar](128) NULL,
	[StatusID] [int] NULL,
	[ReplicationStatus] [varchar](11) NOT NULL,
	[WarningID] [int] NULL,
	[WarningStatus] [varchar](20) NOT NULL,
	[Last_DistSync] [datetime] NULL,
	[MergePerformance] [int] NULL,
	[MergeRunSpeed] [float] NULL,
	[Latency] [int] NULL,
	[DateAdded] [smalldatetime] NULL
)



  BEGIN
	DECLARE @cmd NVARCHAR(max)
	DECLARE @publisher SYSNAME, @publisher_db SYSNAME, @publication SYSNAME, @pubtype INT
	DECLARE @subscriber SYSNAME, @subscriber_db SYSNAME, @subtype INT

IF OBJECT_ID('tempdb..##PublisherInfo') IS NOT NULL
	DROP TABLE ##PublisherInfo;
	

	IF SUBSTRING(CONVERT(VARCHAR(50),SERVERPROPERTY('productversion'),0),1,2) IN ('10')
		SET @cmd = CONCAT('SELECT * INTO ##PublisherInfo
			FROM OPENROWSET(''SQLOLEDB'', ''SERVER=',@Distributer,';TRUSTED_CONNECTION=YES;''
			, ''SET FMTONLY OFF EXEC distribution.dbo.sp_replmonitorhelppublisher'')')
	ELSE 
		SET @cmd = CONCAT('SELECT * INTO ##PublisherInfo
			FROM OPENROWSET(''SQLOLEDB'', ''SERVER=',@Distributer ,';TRUSTED_CONNECTION=YES;''
			, ''SET FMTONLY OFF EXEC distribution.dbo.sp_replmonitorhelppublisher WITH RESULT SETS ((
				publisher nvarchar(128), 
				distribution_db nvarchar(128), 
				[status] int, 
				warning int, 
				publicationcount int, 
				returnstamp nvarchar(128)
			))'')')

    --select @cmd
	EXEC sp_executesql @cmd

	SELECT @publisher = publisher FROM ##PublisherInfo

	--select * from ##PublisherInfo

IF OBJECT_ID('tempdb..##PublicationInfo') IS NOT NULL
	DROP TABLE ##PublicationInfo

	IF SUBSTRING(CONVERT(VARCHAR(50),SERVERPROPERTY('productversion'),0),1,2) IN ('10')
		SET @cmd = CONCAT('SELECT * INTO ##PublicationInfo FROM OPENROWSET(''SQLOLEDB'',''SERVER=', @Distributer,';TRUSTED_CONNECTION=YES''
			,''SET FMTONLY OFF EXEC distribution.dbo.sp_replmonitorhelppublication @publisher=''', @publisher ,''' '')')
	ELSE
		SET @cmd = CONCAT('SELECT * INTO ##PublicationInfo FROM OPENROWSET(''SQLOLEDB'',''SERVER=', @Distributer,';TRUSTED_CONNECTION=YES''
			,''SET FMTONLY OFF EXEC distribution.dbo.sp_replmonitorhelppublication @publisher='''''
			, @publisher , ''''' WITH RESULT SETS ((
				publisher_db varchar(100) null,
				publication varchar(MAX) null,
				publication_id int null,
				publication_type int null,
				status int null,
				warning int null,
				worst_latency int null,
				best_latency int null,
				average_latency int null,
				last_distsync datetime null,
				retention int null,
				latencythreshold int null,
				expirationthreshold int null,
				agentnotrunningthreshold int null,
				subscriptioncount int null,
				runningdisagentcount int null,
				snapshot_agentname varchar(MAX) null,
				logreader_agentname varchar(MAX) null,
				qreader_agentname varchar(255) null,
				worst_runspeedPerf int null,
				best_runspeedPerf int null,
				average_runspeedPerf int null,
				retention_period_unit int null,
				publisher varchar(150) null
				))'')')

	--select @cmd
	EXEC sp_executesql @cmd

	IF OBJECT_ID('tempdb..##PublicationInfo') IS NOT NULL
	  BEGIN
	SELECT @publisher_db=publisher_db, @publication=publication, @pubtype=publication_type  FROM ##PublicationInfo

	--select * from ##PublicationInfo

IF OBJECT_ID('tempdb..##SubscriptionInfo') IS NOT NULL
	DROP TABLE ##SubscriptionInfo

	IF SUBSTRING(CONVERT(VARCHAR(50),SERVERPROPERTY('productversion'),0),1,2) IN ('10')
		SET @cmd = CONCAT('SELECT * INTO ##SubscriptionInfo FROM OPENROWSET(''SQLOLEDB'',''SERVER=', @Distributer,';TRUSTED_CONNECTION=YES''
			,''SET FMTONLY OFF EXEC distribution.dbo.sp_replmonitorhelpsubscription @publisher='''
			, @publisher , ''' ,@publication_type=''' , CONVERT(CHAR(1),@pubtype) , ''' '')')
	ELSE
		SET @cmd = CONCAT('SELECT * INTO ##SubscriptionInfo FROM OPENROWSET(''SQLOLEDB'',''SERVER=', @Distributer,';TRUSTED_CONNECTION=YES''
			,''SET FMTONLY OFF EXEC distribution.dbo.sp_replmonitorhelpsubscription @publisher='''''
			, @publisher , ''''' ,@publication_type=''''' , CONVERT(CHAR(1),@pubtype) , ''''' WITH RESULT SETS ((
					[status] int null,
					warning int null ,
					subscriber sysname null ,
					subscriber_db sysname null ,
					publisher_db sysname null ,
					publication sysname null ,
					publication_type int null ,
					subtype int null ,
					latency int null ,
					latencythreshold int null ,
					agentnotrunning int null ,
					agentnotrunningthreshold  int null ,
					timetoexpiration  int null ,
					expirationthreshold  int null ,
					last_distsync  datetime null ,
					distribution_agentname  sysname null ,
					mergeagentname  sysname null ,
					mergesubscriptionfriendlyname  sysname null ,
					mergeagentlocation  sysname null ,
					mergeconnectiontype  int null ,
					mergePerformance  int null ,
					mergerunspeed float null ,
					mergerunduration int null ,
					monitorranking  int null ,
					distributionagentjobid  binary(30) null ,
					mergeagentjobid binary(30) null ,
					distributionagentid  int null ,
					distributionagentprofileid int null ,
					mergeagentid int null ,
					mergeagentprofileid int null ,
					logreaderagentname sysname null,
					publisher sysname
					))'')'
					)
	--select @cmd
	EXEC sp_executesql @cmd

	--Show Results
	INSERT INTO #ReplicationStatus
	SELECT @@SERVERNAME AS InstanceName
		,@publisher AS Publisher
		,Subscriber
		,CASE WHEN MergeSubscriptionFriendlyName IS NULL THEN publication ELSE MergeSubscriptionFriendlyName END AS FriendlyName
		,[Status] AS StatusID
		,CASE 
			WHEN [status] = 1 THEN 'Started'
			WHEN [status] = 2 THEN 'Stopped'
			WHEN [status] = 3 THEN 'In Progress'
			WHEN [status] = 4 THEN 'Idle'
			WHEN [status] = 5 THEN 'Retrying'
			WHEN [status] = 6 THEN 'Failed'
			ELSE 'Unspecified'
		END AS 'ReplicationStatus'
		,Warning AS WarningID
		,CASE 
			WHEN Warning = 0 THEN 'None'
			WHEN Warning = 1 THEN 'Expiration'
			WHEN Warning = 2 THEN 'Latency'
			WHEN Warning = 4 THEN 'MergeExpiration'
			WHEN Warning = 8 THEN 'MergeFastRunDuration'
			WHEN Warning = 16 THEN 'MergeSlowRunDuration'
			WHEN Warning = 32 THEN 'MergeFastRunSpeed'
			WHEN Warning = 64 THEN 'MergeSlowRunSpeed'
			ELSE 'Unspecified'
		END AS WarningStatus
		,Last_DistSync
		,MergePerformance
		,MergeRunSpeed
		,Latency
		,GETDATE() AS DateAdded
	FROM ##SubscriptionInfo


	--Drop Working Tables
	
	DROP TABLE ##PublicationInfo
	DROP TABLE ##SubscriptionInfo

	END
	DROP TABLE ##PublisherInfo
  END

--SELECT * FROM #ReplicationStatus

/*
SELECT --InstanceName,
Publisher,
Subscriber,
FriendlyName,
StatusID,
ReplicationStatus,
WarningID,
WarningStatus,
Last_DistSync,
--MergePerformance,
--MergeRunSpeed,
Latency,
DateAdded
FROM #ReplicationStatus
*/

  BEGIN
	SET @TSQL = '
		SELECT Publisher
		,Subscriber
		,ISNULL(FriendlyName,'''') AS PublicationName
		,ReplicationStatus
		,WarningStatus
		,ISNULL(Latency,0) AS Latency
		,Last_DistSync AS LastSync
		,DateAdded AS DateCaptured
	FROM #ReplicationStatus
	'

	EXEC Ascent_DBA..usp_QueryToHtmlTable @html = @HTML OUTPUT
		,@query = @TSQL
		,@orderby = 'ORDER BY Publisher,Latency DESC,PublicationName'

	SET @FullHTML = @FullHTML+ '<br /><H3>Replication Status:</H3>'+ISNULL(@HTML,'<br /><br />')
  END


	DECLARE @WarningCount int;
	SELECT @WarningCount = COUNT(WarningID) FROM #ReplicationStatus WHERE WarningID = 2 OR StatusID = 5

	IF @WarningCount > 0
	BEGIN
		SELECT @FullHTML = REPLACE(@FullHTML,'background: #b1f4da;','background: #edbb6b;')
		SELECT @FullHTML = REPLACE(@FullHTML,'border-top: 7px solid #6eebac;','border-top: 7px solid #ff892e;')
	END

  	DECLARE @ErrorCount int;
	SELECT @ErrorCount = COUNT(WarningID) FROM #ReplicationStatus WHERE WarningID NOT IN(0,2) OR StatusID IN(2,6)

	IF @ErrorCount > 0
	BEGIN
		SELECT @FullHTML = REPLACE(@FullHTML,'background: #b1f4da;','background: #ff7a7a;')
		SELECT @FullHTML = REPLACE(@FullHTML,'border-top: 7px solid #6eebac;','border-top: 7px solid #ff3232;')
	END
--SELECT @FullHTML

-----------------
--Send the Mail--
-----------------


DECLARE @MailSubject VARCHAR(50)
SET @MailSubject = @CompanyName+' Replication Status Report - ' + CONVERT(VARCHAR(20),GETDATE(),106)

EXEC msdb..sp_send_dbmail @recipients = @Mailto
	,@body = @FullHTML
	,@body_format = 'HTML'
	,@subject = @MailSubject
	,@query_no_truncate = 1
	,@attach_query_result_as_file = 0;



--Select * from msdb.dbo.sysmail_allitems order by mailitem_id desc

END
