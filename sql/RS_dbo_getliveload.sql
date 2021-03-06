USE [ResearchScience]
GO
/****** Object:  StoredProcedure [dbo].[uspScheduling_GetLiveLoad]    Script Date: 12/31/2019 10:29:52 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
ALTER PROCEDURE [dbo].[uspScheduling_GetLiveLoad] 
	-- Add the parameters for the stored procedure here
	@LoadUpdateDateTime DATETime 
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
    SET XACT_ABORT ON;

    -- Insert statements for procedure here
	SELECT 
	L.ID As LoadID
	,L.LoadDate
	,L.Miles
	,L.TotalWeight
	,lcus.CustomerID
	,LSP.FacilityID AS   PU_Facility , 
	LSP.CityID	AS PU_City, 
	LSP.ScheduleType AS   PU_ScheduleType,  
	LSP.ScheduleCloseTime AS   PU_ScheduleCloseTime, 
	--LSP.ReadyDate AS   PU_ReadyDate,  
	LSP.LoadByDate AS   PU_LoadByDate,
    LSP.CloseTime AS PU_time,
	LSD.facilityID AS   DO_Facility, 
	LSD.cityID AS   DO_City, 
	LSD.ScheduleType AS   DO_ScheduleType,  
	LSD.ScheduleCloseTime  AS   DO_ScheduleCloseTime, 
	LSD.LoadByDate AS   DO_LoadByDate,
	LSD.CloseTime AS DO_time,
	L.UpdateDate
	FROM Bazooka.dbo.[Load] L
	INNER JOIN Bazooka.dbo.LoadCustomer LCUS ON LCUS.LoadID = L.ID AND LCUS.Main = 1
	INNER JOIN Bazooka.dbo.LoadStop LSP ON LSP.ID = L.OriginLoadStopID
	INNER JOIN Bazooka.dbo.LoadStop LSD ON LSD.ID = L.DestinationLoadStopID
	WHERE 
	 L.UpdateDate >= @LoadUpdateDateTime  
	 and L.LoadDate >= getdate()
	AND L.statetype = 3  -- on hold load
	AND L.NumStops=2 AND L.Mode = 1 -- AND L.LoadDate=@date  
	and L.Miles>0 
	AND L.TotalRate > 150 
	AND L.Division <>5
	AND L.ShipmentType NOT IN (3, 4, 6, 7)
	AND (LSP.ScheduleType-1)*( LSD.ScheduleType-1)=0
	--and (LSP.ScheduleCloseTime ='1753-01-01' OR LSD.ScheduleCloseTime ='1753-01-01')
END
