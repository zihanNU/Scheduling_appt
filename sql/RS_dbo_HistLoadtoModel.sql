USE [ResearchScience]
GO
/****** Object:  StoredProcedure [dbo].[uspScheduling_HistLoadtoModel]    Script Date: 12/31/2019 10:22:16 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
ALTER PROCEDURE [dbo].[uspScheduling_HistLoadtoModel] 
	-- Add the parameters for the stored procedure here
	@StartDate DATE 
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
    SET XACT_ABORT ON;
 
	 SELECT 
      LoadID
      ,LoadDate
      ,Miles
      ,TotalWeight
      ,LoadEquipment
      ,ActualEquipmentType
      ,CarrierID
      ,CustomerID
      ,puFacilityID As PU_Facility
      ,puCityid  AS PU_City
      ,puScheduleType  As PU_ScheduleType
      ,puAppointment  AS PU_Appt
      ,puArriveDateTime AS PU_Arrive
      ,puDepartDateTime  AS PU_Depart
      ,dlFacilityid As DO_Facility
      ,dlCityid As DO_City
      ,dlScheduleType As DO_ScheduleType
      ,dlAppointment AS DO_Appt
      ,dlArriveDateTime  AS DO_Arrive
      ,dlDepartDateTime AS DO_Depart
      ,datepart(hour,puAppointment) PU_Hour
      ,datepart(hour,dlAppointment) DO_Hour
      ,PU_Bucket
      ,DL_Bucket AS DO_Bucket
  FROM ResearchScience.dbo.Scheduling_histloads
  where LoadDate>= @StartDate  
END
