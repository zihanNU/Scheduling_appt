USE [ResearchScience]
GO
/****** Object:  StoredProcedure [dbo].[uspScheduling_FacilityHour]    Script Date: 12/31/2019 10:32:26 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
ALTER PROCEDURE [dbo].[uspScheduling_FacilityHour]
 
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
    SET XACT_ABORT ON;

    select 
ID As FacilityID, 
convert(time,MondayOpen)   as 'MondayOpen',
convert(time,MondayClose)	as 'MondayClose',
convert(time,TuesdayOpen)  as 'TuesdayOpen',
convert(time,TuesdayClose) as 'TuesdayClose',
convert(time,WednesdayOpen) as 'WednesdayOpen',
convert(time,WednesdayClose) as 'WednesdayClose',
convert(time,ThursdayOpen) as 'ThursdayOpen',
convert(time,ThursdayClose) as 'ThursdayClose',
convert(time,FridayOpen) as 'FridayOpen',
convert(time,FridayClose) as 'FridayClose',
convert(time,SaturdayOpen) as 'SaturdayOpen',
convert(time,SaturdayClose) as 'SaturdayClose',
convert(time,SundayOpen) as 'SundayOpen',
convert(time,SundayClose)  as 'SundayClose'，
UpdateDateUTC
from bazooka.dbo.facility
END
