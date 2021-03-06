USE [ResearchScience]
GO
/****** Object:  StoredProcedure [dbo].[uspScheduling_CityInfo]    Script Date: 12/31/2019 10:32:06 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
ALTER PROCEDURE [dbo].[uspScheduling_CityInfo]
AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
    SET XACT_ABORT ON;

	SELECT lc.CityID,C.Name,
		   C.Latitude,C.Longitude,
		   ltz.IANATimeZoneName AS TimeZone,
		   lc.UpdateDate
	FROM Bazooka.dbo.LocationCity lc
	INNER JOIN Bazooka.dbo.LocationTimeZone ltz ON lc.LocationTimeZoneId = ltz.LocationTimeZoneId
	INNER JOIN Bazooka.dbo.City C on C.id=lc.CityId
	WHERE C.stateid <=320 
	 ;
END
