import pyodbc

from setting.config import login_db, password_db, db, server_db


def sql_server_start():
    global conn, cursor
    conn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                          f"Server={server_db};"
                          f"Database={db};"
                          # "Trusted_Connection=yes")
                          f"UID={login_db};"
                          f"PWD={password_db};")
    print(conn)
    cursor = conn.cursor()


# рабочие
async def read_list_gas_station():
    result_query_db = cursor.execute("""
    SELECT *
    FROM [Essenza].[dbo].[GasStations]
   WHERE  IsInControlSystem = 1
    """)

    return result_query_db


async def search_gas_station_id(number):
    result_query_db = cursor.execute("""SELECT [GasStationId]
                                     FROM [Essenza].[dbo].[GasStations]
                                     where GasStationNumber = ?
                                     and IsInControlSystem = 1 """, number)

    gas_statin_id = None
    for row in result_query_db:
        gas_statin_id = row.GasStationId
    return gas_statin_id


async def read_gas_station(gs_id):
    result_query_db = cursor.execute("""
    SELECT *
    FROM [Essenza].[dbo].[GasStations]
    WHERE GasStationId = ? and IsInControlSystem = 1
    """, gs_id)

    dict_gas_station = {}

    for row in result_query_db:
        dict_gas_station['GasStationId'] = row.GasStationId
        dict_gas_station['GpsLatitude'] = float(row.GpsLatitude.replace(',', '.'))
        dict_gas_station['GpsLongitude'] = float(row.GpsLongitude.replace(',', '.'))
        dict_gas_station['message_gas_station'] = f"<b>{row.GasStationName}</b>\n\n" \
                                                  f"<b>Адрес: </b>{row.Address}\n" \
                                                  f"<b>Телефон: </b>{row.PhoneForMobile} \n\n" \
                                                  f"<b>Резервуары: </b>"

    return dict_gas_station


async def read_fuel_tanks_list(gs_id):
    result_query_db = cursor.execute("""
    SELECT ftn.[FuelTankId]
           , ftn.[FuelTankNumber]
           , ftn.[FuelTypeId]
           , ftn.[GasStationId]
	       , ftp.[FuelTypeName]
	FROM [Essenza].[dbo].[FuelTanks] as ftn
    join [Essenza].[dbo].[FuelTypes] as ftp ON ftn.[FuelTypeId] = ftp.FuelTypeId
    where ftn.[GasStationId] = ? and (ftn.IsDisabled = 0 or ftn.IsDisabled is null)
    order by ftn.[FuelTankNumber]
    """, gs_id)

    return result_query_db


async def read_procedure_current_fuel(gs_id, tank_id):
    # result_proc_db = cursor.execute("exec sp_report_current_fuel_tanks_data_NEW2_AV ")
    if tank_id == 0:
        result_proc_db = cursor.execute(''' 
        select  tankData. *
        from (select * from [Essenza].[dbo].[FuelTanks] where GasStationId = ?) as tank -- @GasStationId
        cross apply (
            select top 1
                gs.GasStationName
                , gs.Address
                , ft.FuelTankNumber
                , ftp.FuelTypeName
                , lmd.[Timestamp]
                , ft.MaximumCapacity
                , lmd.Litres
                , ft.DeadVolume
                , CASE WHEN ft.MaximumCapacity = 0 THEN NULL ELSE ISNULL(tanks_data.Litres, LMD.Litres) /  (ISNULL(ft.UpperLimit, ft.MaximumCapacity) )*100 END as 'Percent'
                , ft.MaximumCapacity / NULLIF(lmd.Litres,0) as 'Color'
                , lmd.[Weight]
                , lmd.Temperature
                , lmd.Density
                , lmd.[Level]
                , lmd.Water
                , (select top 1 FST.Name 
                   FROM [Essenza].[dbo].[FuelReceptions] (nolock) FR 
                   left join [Essenza].[dbo].[FuelReceptionWebInfoSections] FRWIS on FR.WebInfoSectionId = FRWIS.Id 
                   left join [Essenza].[dbo].[FuelSubTypes] (nolock) FST on FST.Id = FRWIS.FuelSubTypeId
                   WHERE FR.FuelTankId = tank.FuelTankId
                   ORDER BY fr.CreateDate desc) AS LastType
                , (select STUFF((select  N'/' + fst.Name + N'(' + cast(ROUND(fsts.Share * 100, 0) as nvarchar) + '%)'
                   from [Essenza].[dbo].[FuelSubTypesShares] (nolock) fsts
                   join [Essenza].[dbo].[FuelSubTypes] (nolock) fst on fst.Id = fsts.FuelSubTypeId
                   cross apply (select top 1 * 
                                from [Essenza].[dbo].[FuelSubTypesShares] (nolock)
                                where GasStationId = gs.GasStationId and FuelTankId = ft.FuelTankId
                                order by CreateDate desc) wis
                                where wis.FuelReceptionWebInfoSectionId = fsts.FuelReceptionWebInfoSectionId and wis.GasStationId = fsts.GasStationId
                                      and fst.FuelTypeId = ftp.FuelTypeId
                            FOR XML PATH('')), 1, 1, ''))  as 'TypeRatio'
                , CASE WHEN FTB.RefuelingChangeId IS NULL THEN 'Нет данных' 
                       WHEN OpenedWithMeasureStick = 1 OR ClosedWithMeasureStick = 1 THEN 'По метроштоку' ELSE 'По уровнемеру' 
                       END AS ValueTypeName
                , CASE WHEN FTB.OpenedWithMeasureStick = 1 OR FTB.ClosedWithMeasureStick = 1 THEN 1 ELSE 0 END AS Warning
                , CASE WHEN FTB.OpenedWithMeasureStick = 1 THEN 'По метроштоку'
                       WHEN FTB.RefuelingChangeId IS NULL THEN 'Нет данных' ELSE 'По уровнемеру'
                       END  AS ChangeOpeningTypeName
            
            from [Essenza].[dbo].[GasStations] as gs
            left join [Essenza].[dbo].[FuelTankParamHistory2] as lmd on gs.GasStationId = lmd.GasStationId
            left join [Essenza].[dbo].[FuelTanks] as ft on ft.FuelTankId = lmd.FuelTankId
            left join [Essenza].[dbo].[FuelTypes] as ftp on ftp.FuelTypeId = ft.FuelTypeId
            left join [Essenza].[dbo].[FuelSubTypes] (nolock) fst on fst.FuelTypeId = ftp.FuelTypeId
            LEFT JOIN (select *
                        -- ОТКРЫВАЛАСЬ ли смена по метроштоку:
                        ,IIF(BeginFuelMeasureStickId IS NOT NULL, 1, 0) AS OpenedWithMeasureStick
                        -- ЗАКРЫВАЛАСЬ ли смена по метроштоку:
                        ,IIF([EndFuelMeasureStickId] IS NOT NULL, 1, 0) AS ClosedWithMeasureStick
                        from [Essenza].[dbo].[RefuelingChangeFuelTankBalances] (nolock) ) as FTB ON  FTB.FuelTankId = FT.FuelTankId
            OUTER APPLY (
                    select top 1 CreateDate, Litres
                    from [Essenza].[dbo].[FuelTankParamHistory2]  (nolock)     history 
                    where history.FuelTankId = ft.FuelTankId 
                    order by CreateDate desc
                    )  tanks_data
            where 
                ft.FuelTankId = tank.FuelTankId and 
                lmd.Litres is not null
                and (
                        ft.IsDisabled = 0 or ft.IsDisabled is null
                )
            -- and lmd.[Timestamp] >= dateadd(day,datediff(day,2,GETDATE()),0) 
                order by lmd.[Timestamp] desc
        ) as tankData 
        order by tankData.FuelTankNumber
        ''', gs_id)
    else:
        result_proc_db = cursor.execute(''' 
        select  tankData. *
        from (
            select * from [Essenza].[dbo].[FuelTanks] where GasStationId = ? and FuelTankId = ?
            ) as tank -- @GasStationId
        cross apply (
            select top 1
                gs.GasStationName
                , gs.[Address]
                , ft.FuelTankNumber
                , ftp.FuelTypeName
                , lmd.[Timestamp]
                , ft.MaximumCapacity
                , lmd.Litres
                , ft.DeadVolume
                , CASE WHEN ft.MaximumCapacity = 0 THEN NULL ELSE ISNULL(tanks_data.Litres, LMD.Litres) /  (ISNULL(ft.UpperLimit, ft.MaximumCapacity) )*100 END as 'Percent'
                , ft.MaximumCapacity / NULLIF(lmd.Litres,0) as 'Color'
                , lmd.[Weight]
                , lmd.Temperature
                , lmd.Density
                , lmd.[Level]
                , lmd.Water
                , (select top 1 FST.Name 
                   FROM [Essenza].[dbo].[FuelReceptions] (nolock) FR 
                   left join [Essenza].[dbo].[FuelReceptionWebInfoSections] FRWIS on FR.WebInfoSectionId = FRWIS.Id 
                   left join [Essenza].[dbo].[FuelSubTypes] (nolock) FST on FST.Id = FRWIS.FuelSubTypeId
                   WHERE FR.FuelTankId = tank.FuelTankId
                   ORDER BY fr.CreateDate desc) AS LastType
                , (select STUFF((select  N'/' + fst.Name + N'(' + cast(ROUND(fsts.Share * 100, 0) as nvarchar) + '%)'
                   from [Essenza].[dbo].[FuelSubTypesShares] (nolock) fsts
                   join [Essenza].[dbo].[FuelSubTypes] (nolock) fst on fst.Id = fsts.FuelSubTypeId
                   cross apply (select top 1 * 
                                from [Essenza].[dbo].[FuelSubTypesShares] (nolock)
                                where GasStationId = gs.GasStationId and FuelTankId = ft.FuelTankId
                                order by CreateDate desc) wis
                                where wis.FuelReceptionWebInfoSectionId = fsts.FuelReceptionWebInfoSectionId and wis.GasStationId = fsts.GasStationId
                                      and fst.FuelTypeId = ftp.FuelTypeId
                            FOR XML PATH('')), 1, 1, ''))  as 'TypeRatio'
                , CASE WHEN FTB.RefuelingChangeId IS NULL THEN 'Нет данных' 
                       WHEN OpenedWithMeasureStick = 1 OR ClosedWithMeasureStick = 1 THEN 'По метроштоку' ELSE 'По уровнемеру' 
                       END AS ValueTypeName
                , CASE WHEN FTB.OpenedWithMeasureStick = 1 OR FTB.ClosedWithMeasureStick = 1 THEN 1 ELSE 0 END AS Warning
                , CASE WHEN FTB.OpenedWithMeasureStick = 1 THEN 'По метроштоку'
                       WHEN FTB.RefuelingChangeId IS NULL THEN 'Нет данных' ELSE 'По уровнемеру'
                        END  AS ChangeOpeningTypeName
            from [Essenza].[dbo].[GasStations] as gs
            left join [Essenza].[dbo].[FuelTankParamHistory2] as lmd on gs.GasStationId = lmd.GasStationId
            left join [Essenza].[dbo].[FuelTanks] as ft on ft.FuelTankId = lmd.FuelTankId
            left join [Essenza].[dbo].[FuelTypes] as ftp on ftp.FuelTypeId = ft.FuelTypeId
            left join [Essenza].[dbo].[FuelSubTypes] (nolock) fst on fst.FuelTypeId = ftp.FuelTypeId
            LEFT JOIN (select *
                        -- ОТКРЫВАЛАСЬ ли смена по метроштоку:
                        ,IIF(BeginFuelMeasureStickId IS NOT NULL, 1, 0) AS OpenedWithMeasureStick
                        -- ЗАКРЫВАЛАСЬ ли смена по метроштоку:
                        ,IIF([EndFuelMeasureStickId] IS NOT NULL, 1, 0) AS ClosedWithMeasureStick
                        from [Essenza].[dbo].[RefuelingChangeFuelTankBalances] (nolock) ) as FTB ON  FTB.FuelTankId = FT.FuelTankId
            OUTER APPLY (
                select top 1 CreateDate, Litres
                from [Essenza].[dbo].[FuelTankParamHistory2]  (nolock)     history 
                where history.FuelTankId = ft.FuelTankId 
                order by CreateDate desc
            )    tanks_data
            where 
                ft.FuelTankId = tank.FuelTankId and 
                lmd.Litres is not null
                and (
                        ft.IsDisabled = 0 or ft.IsDisabled is null
                )
            -- and lmd.[Timestamp] >= dateadd(day,datediff(day,2,GETDATE()),0) 
            order by lmd.[Timestamp] desc
        ) as tankData
        order by tankData.FuelTankNumber
        ''', gs_id, tank_id)

    str_fuel_tank = []
    for row in result_proc_db:
        data = row.Timestamp
        time_format = "%d.%m.%Y %H:%M:%S"
        litres = round(row.Litres, 2)
        dead_volume_litres = '-' if row.DeadVolume is None else round(row.DeadVolume)
        percent = round(row.Percent, 2)
        weight = '-' if row.Weight is None else round(row.Weight, 2)
        temperature = '-' if row.Temperature is None else round(row.Temperature, 2)
        density = '-' if row.Density is None else round(row.Density, 2)
        level = '-' if row.Level is None else round(row.Level, 2)
        water = '-' if row.Water is None else round(row.Water, 2)

        last_type = '' if row.LastType is None else f'\n\n<b>Последний слитый тип:</b> {row.LastType}\n'
        type_ratio = "" if row.TypeRatio is None else f"<b>Соотношение типов:</b> {row.TypeRatio}"

        value_type_name = row.ValueTypeName
        warning = "‼‼" if row.Warning == 1 else f""

        progress_bar = '‼‼ <b>Нет данных</b> ‼‼' if row.Color is None else await fn_progress_bar(percent, row.Color)

        str_fuel_tank.append(f"<b>{row.GasStationName}</b>\n\n"
                             f"<b>Резервуар: {row.FuelTankNumber}</b>  ⛽ {row.FuelTypeName}\n\n"
                             f"<b>Дата замера:</b> {data:{time_format}}\n\n"
                             f"<b>Остаток литров:</b> <i><b>{litres}</b></i>\n"
                             f"<b>Ёмкость:</b> {row.MaximumCapacity} литров\n"
                             f"<b>Мертвый остаток:</b> {dead_volume_litres}\n"
                             # f"<b>Процент заполнения:</b> {percent} %\n"
                             f"<b>Вес:</b> {weight}\n"
                             f"<b>Температура:</b> {temperature}\n"
                             f"<b>Плотность:</b> {density}\n"
                             f"<b>Уровень:</b> {level}\n"
                             f"<b>Вода:</b> {water}"
                             f"{last_type}{type_ratio}"
                             f"\n\n{warning}{value_type_name}"
                             f"\n<b>Заполненость:</b>\n"
                             f"{progress_bar} {percent}%\n")

    return str_fuel_tank


async def fn_progress_bar(percent, color):
    pr_bar = ''
    part = round(percent * 10 / 100)
    part_grey = 10 - part

    pr_bar = '🔻' if part < 0.5 else ''

    if color >= 5:
        part_color = "🟥"
    elif color < 2:
        part_color = '🟩'
    elif color >= 2:
        part_color = '🟨'
    else:
        part_color = "⬛"

    for i in range(part):
        pr_bar = pr_bar + part_color
    for i in range(part_grey):
        pr_bar = pr_bar + '⬜'

    return pr_bar


async def read_values_mo(values):
    circle_emoji = ""
    result_query_db = None
    if values == "average":
        circle_emoji = "🟡"
        result_query_db = cursor.execute("""
            select  tankData. *
            from (
                    select ftss.FuelTankId, gss.GasStationId
                    from [Essenza].[dbo].[FuelTanks] as ftss
                    join [Essenza].[dbo].[GasStations] as gss on gss.GasStationId = ftss.GasStationId
                    where  gss.IsInControlSystem = 1 and
                    gss.GasStationNumber like'0%') as tank -- @GasStationId
            cross apply (
                select top 1	
                     gs.GasStationName
                    , ft.FuelTankNumber
                    , ftp.FuelTypeName
                    , lmd.[Timestamp]
                    , ft.MaximumCapacity
                    , lmd.Litres
                    , CASE WHEN ft.MaximumCapacity = 0 THEN NULL ELSE ISNULL(tanks_data.Litres, LMD.Litres) /  (ISNULL(ft.UpperLimit, ft.MaximumCapacity) )*100 END as 'Percent'
                    , ft.MaximumCapacity / NULLIF(lmd.Litres,0) as 'Color'
                    , CASE WHEN FTB.RefuelingChangeId IS NULL THEN 'Нет данных' 
                           WHEN OpenedWithMeasureStick = 1 OR ClosedWithMeasureStick = 1 THEN 'По метроштоку' ELSE 'По уровнемеру' 
                           END AS ValueTypeName
                    , CASE WHEN FTB.OpenedWithMeasureStick = 1 OR FTB.ClosedWithMeasureStick = 1 THEN 1 ELSE 0 END AS Warning
                from [Essenza].[dbo].[GasStations] as gs
                left join [Essenza].[dbo].[FuelTankParamHistory2] as lmd on gs.GasStationId = lmd.GasStationId
                left join [Essenza].[dbo].[FuelTanks] as ft on ft.FuelTankId = lmd.FuelTankId
                left join [Essenza].[dbo].[FuelTypes] as ftp on ftp.FuelTypeId = ft.FuelTypeId
                LEFT JOIN (select FuelTankId, RefuelingChangeId
                            -- ОТКРЫВАЛАСЬ ли смена по метроштоку:
                            ,IIF(BeginFuelMeasureStickId IS NOT NULL, 1, 0) AS OpenedWithMeasureStick
                            -- ЗАКРЫВАЛАСЬ ли смена по метроштоку:
                            ,IIF([EndFuelMeasureStickId] IS NOT NULL, 1, 0) AS ClosedWithMeasureStick
                            from [Essenza].[dbo].[RefuelingChangeFuelTankBalances] (nolock) ) as FTB ON  FTB.FuelTankId = FT.FuelTankId
                OUTER APPLY (
                    select top 1 CreateDate, Litres
                    from [Essenza].[dbo].[FuelTankParamHistory2]  (nolock)     history 
                    where history.FuelTankId = ft.FuelTankId 
                    order by   CreateDate desc
                )      tanks_data
            where 
                ft.FuelTankId = tank.FuelTankId and 
                lmd.Litres is not null
                and (
                        ft.IsDisabled = 0 or ft.IsDisabled is null
                )
                order by lmd.[Timestamp] desc
            ) as tankData
            where tankData.Color >= 2 and tankData.Color <= 5
            order by tank.GasStationid
        """)

    elif values == "critical":
        circle_emoji = "🔴"
        result_query_db = cursor.execute("""
            select  tankData. *
            from (
                    select ftss.FuelTankId, gss.GasStationId
                    from [Essenza].[dbo].[FuelTanks] as ftss
                    join [Essenza].[dbo].[GasStations] as gss on gss.GasStationId = ftss.GasStationId
                    where  gss.IsInControlSystem = 1 and
                    gss.GasStationNumber like'0%') as tank -- @GasStationId
            cross apply (
                select top 1	
                     gs.GasStationName
                    , ft.FuelTankNumber
                    , ftp.FuelTypeName
                    , lmd.[Timestamp]
                    , ft.MaximumCapacity
                    , lmd.Litres
                    , CASE WHEN ft.MaximumCapacity = 0 THEN NULL ELSE ISNULL(tanks_data.Litres, LMD.Litres) /  (ISNULL(ft.UpperLimit, ft.MaximumCapacity) )*100 END as 'Percent'
                    , ft.MaximumCapacity / NULLIF(lmd.Litres,0) as 'Color'
                    , CASE WHEN FTB.RefuelingChangeId IS NULL THEN 'Нет данных' 
                           WHEN OpenedWithMeasureStick = 1 OR ClosedWithMeasureStick = 1 THEN 'По метроштоку' ELSE 'По уровнемеру' 
                           END AS ValueTypeName
                    , CASE WHEN FTB.OpenedWithMeasureStick = 1 OR FTB.ClosedWithMeasureStick = 1 THEN 1 ELSE 0 END AS Warning
                from [Essenza].[dbo].[GasStations] as gs
                left join [Essenza].[dbo].[FuelTankParamHistory2] as lmd on gs.GasStationId = lmd.GasStationId
                left join [Essenza].[dbo].[FuelTanks] as ft on ft.FuelTankId = lmd.FuelTankId
                left join [Essenza].[dbo].[FuelTypes] as ftp on ftp.FuelTypeId = ft.FuelTypeId
                LEFT JOIN (select FuelTankId, RefuelingChangeId
                            -- ОТКРЫВАЛАСЬ ли смена по метроштоку:
                            ,IIF(BeginFuelMeasureStickId IS NOT NULL, 1, 0) AS OpenedWithMeasureStick
                            -- ЗАКРЫВАЛАСЬ ли смена по метроштоку:
                            ,IIF([EndFuelMeasureStickId] IS NOT NULL, 1, 0) AS ClosedWithMeasureStick
                            from [Essenza].[dbo].[RefuelingChangeFuelTankBalances] (nolock) ) as FTB ON  FTB.FuelTankId = FT.FuelTankId
                OUTER APPLY (
                        select top 1 CreateDate, Litres
                        from [Essenza].[dbo].[FuelTankParamHistory2]  (nolock)     history 
                        where history.FuelTankId = ft.FuelTankId 
                        order by CreateDate desc
                )  tanks_data
                where 
                    ft.FuelTankId = tank.FuelTankId and 
                    lmd.Litres is not null
                    and (
                            ft.IsDisabled = 0 or ft.IsDisabled is null
                    )
                order by lmd.[Timestamp] desc
            ) as tankData
            where tankData.Color >= 5 
            order by tank.GasStationid
        """)

    str_fuel_tank = []
    str_fuel_tank_var = []
    counter = 0

    for row in result_query_db:
        warning = "❗" if row.Warning == 1 else f""
        if counter < 50:
            str_fuel_tank.append(f"{circle_emoji} {row.GasStationName}  №{row.FuelTankNumber}  {row.FuelTypeName}\n"
                                 f"  <b>{round(row.Litres, 2)}</b>/{row.MaximumCapacity}  <b>{round(row.Percent, 2)}%</b> {warning}{row.ValueTypeName}\n")
            counter = counter + 1
        elif counter >= 50:
            str_fuel_tank_var.append(f"{circle_emoji} {row.GasStationName}  №{row.FuelTankNumber}  {row.FuelTypeName}\n"
                                     f"  <b>{round(row.Litres, 2)}</b>/{row.MaximumCapacity}  <b>{round(row.Percent, 2)}%</b> {warning}{row.ValueTypeName}\n")
            counter = counter + 1

    return str_fuel_tank, str_fuel_tank_var


async def read_values_moscow(values):
    circle_emoji = ""
    result_query_db = None
    if values == "average":
        circle_emoji = "🟡"
        result_query_db = cursor.execute("""
            select  tankData. *
            from (
                    select ftss.FuelTankId, gss.GasStationId
                    from [Essenza].[dbo].[FuelTanks] as ftss
                    join [Essenza].[dbo].[GasStations] as gss on gss.GasStationId = ftss.GasStationId
                    where  gss.IsInControlSystem = 1 and
                    gss.GasStationNumber not like'0%') as tank -- @GasStationId
            cross apply (
                select top 1	
                     gs.GasStationName
                    , ft.FuelTankNumber
                    , ftp.FuelTypeName
                    , lmd.[Timestamp]
                    , ft.MaximumCapacity
                    , lmd.Litres
                    , CASE WHEN ft.MaximumCapacity = 0 THEN NULL ELSE ISNULL(tanks_data.Litres, LMD.Litres) /  (ISNULL(ft.UpperLimit, ft.MaximumCapacity) )*100 END as 'Percent'
                    , ft.MaximumCapacity / NULLIF(lmd.Litres,0) as 'Color'
            
                    , CASE WHEN FTB.RefuelingChangeId IS NULL THEN 'Нет данных' 
                           WHEN OpenedWithMeasureStick = 1 OR ClosedWithMeasureStick = 1 THEN 'По метроштоку' ELSE 'По уровнемеру' 
                           END AS ValueTypeName
                    , CASE WHEN FTB.OpenedWithMeasureStick = 1 OR FTB.ClosedWithMeasureStick = 1 THEN 1 ELSE 0 END AS Warning
                from [Essenza].[dbo].[GasStations] as gs
                left join [Essenza].[dbo].[FuelTankParamHistory2] as lmd on gs.GasStationId = lmd.GasStationId
                left join [Essenza].[dbo].[FuelTanks] as ft on ft.FuelTankId = lmd.FuelTankId
                left join [Essenza].[dbo].[FuelTypes] as ftp on ftp.FuelTypeId = ft.FuelTypeId
                LEFT JOIN (select FuelTankId, RefuelingChangeId
                            -- ОТКРЫВАЛАСЬ ли смена по метроштоку:
                            ,IIF(BeginFuelMeasureStickId IS NOT NULL, 1, 0) AS OpenedWithMeasureStick
                            -- ЗАКРЫВАЛАСЬ ли смена по метроштоку:
                            ,IIF([EndFuelMeasureStickId] IS NOT NULL, 1, 0) AS ClosedWithMeasureStick
                            from [Essenza].[dbo].[RefuelingChangeFuelTankBalances] (nolock) ) as FTB ON  FTB.FuelTankId = FT.FuelTankId
                OUTER APPLY (
                    select top 1 CreateDate, Litres
                    from  [Essenza].[dbo].[FuelTankParamHistory2]  (nolock)     history 
                    where  history.FuelTankId = ft.FuelTankId 
                    order by  CreateDate desc
                )   tanks_data
                where 
                    ft.FuelTankId = tank.FuelTankId and 
                    lmd.Litres is not null
                    and (
                            ft.IsDisabled = 0 or ft.IsDisabled is null
                    )
                order by lmd.[Timestamp] desc
            ) as tankData
            where tankData.Color >= 2 and tankData.Color <= 5
            order by tank.GasStationid
        """)
    elif values == "critical":
        circle_emoji = "🔴"
        result_query_db = cursor.execute("""
            select  tankData. *
            from (
                select ftss.FuelTankId, gss.GasStationId
                from [Essenza].[dbo].[FuelTanks] as ftss
                join [Essenza].[dbo].[GasStations] as gss on gss.GasStationId = ftss.GasStationId
                where  gss.IsInControlSystem = 1 and
                gss.GasStationNumber not like'0%') as tank -- @GasStationId
            cross apply (
                select top 1	
                     gs.GasStationName
                    , ft.FuelTankNumber
                    , ftp.FuelTypeName
                    , lmd.[Timestamp]
                    , ft.MaximumCapacity
                    , lmd.Litres
                    , CASE WHEN ft.MaximumCapacity = 0 THEN NULL ELSE ISNULL(tanks_data.Litres, LMD.Litres) /  (ISNULL(ft.UpperLimit, ft.MaximumCapacity) )*100 END as 'Percent'
                    , ft.MaximumCapacity / NULLIF(lmd.Litres,0) as 'Color'
                    , CASE WHEN FTB.RefuelingChangeId IS NULL THEN 'Нет данных' 
                           WHEN OpenedWithMeasureStick = 1 OR ClosedWithMeasureStick = 1 THEN 'По метроштоку' ELSE 'По уровнемеру' 
                           END AS ValueTypeName
                    , CASE WHEN FTB.OpenedWithMeasureStick = 1 OR FTB.ClosedWithMeasureStick = 1 THEN 1 ELSE 0 END AS Warning
                from [Essenza].[dbo].[GasStations] as gs
                left join [Essenza].[dbo].[FuelTankParamHistory2] as lmd on gs.GasStationId = lmd.GasStationId
                left join [Essenza].[dbo].[FuelTanks] as ft on ft.FuelTankId = lmd.FuelTankId
                left join [Essenza].[dbo].[FuelTypes] as ftp on ftp.FuelTypeId = ft.FuelTypeId
                LEFT JOIN (select FuelTankId, RefuelingChangeId
                            -- ОТКРЫВАЛАСЬ ли смена по метроштоку:
                            ,IIF(BeginFuelMeasureStickId IS NOT NULL, 1, 0) AS OpenedWithMeasureStick
                            -- ЗАКРЫВАЛАСЬ ли смена по метроштоку:
                            ,IIF([EndFuelMeasureStickId] IS NOT NULL, 1, 0) AS ClosedWithMeasureStick
                            from [Essenza].[dbo].[RefuelingChangeFuelTankBalances] (nolock) ) as FTB ON  FTB.FuelTankId = FT.FuelTankId
                OUTER APPLY (
                    select top 1 CreateDate, Litres
                    from [Essenza].[dbo].[FuelTankParamHistory2]  (nolock)     history 
                    where  history.FuelTankId = ft.FuelTankId 
                    order by  CreateDate desc
                )  tanks_data
                where 
                    ft.FuelTankId = tank.FuelTankId and 
                    lmd.Litres is not null
                    and (
                            ft.IsDisabled = 0 or ft.IsDisabled is null
                    )
                order by lmd.[Timestamp] desc
            ) as tankData
            where tankData.Color >= 5 
            order by tank.GasStationid
        """)

    str_fuel_tank = []
    str_fuel_tank_var = []
    counter = 0

    for row in result_query_db:
        warning = "❗" if row.Warning == 1 else f""
        if counter < 50:
            str_fuel_tank.append(f"{circle_emoji} {row.GasStationName}  №{row.FuelTankNumber}  {row.FuelTypeName}\n"
                                 f"  <b>{round(row.Litres, 2)}</b>/{row.MaximumCapacity}  <b>{round(row.Percent, 2)}%</b> {warning}{row.ValueTypeName}\n")
            counter = counter + 1
        elif counter >= 50:
            str_fuel_tank_var.append(f"{circle_emoji} {row.GasStationName}  №{row.FuelTankNumber}  {row.FuelTypeName}\n"
                                     f"  <b>{round(row.Litres, 2)}</b>/{row.MaximumCapacity}  <b>{round(row.Percent, 2)}%</b> {warning}{row.ValueTypeName}\n")
            counter = counter + 1

    return str_fuel_tank, str_fuel_tank_var


# неиспользуемые
async def read_fuel():
    result_query_db = cursor.execute("""
        SELECT *
        FROM [Essenza].[dbo].[FuelTypes]
        """)
    return result_query_db


async def read_procedure_fuel_receptions():
    result_proc_db = cursor.execute("""exec sp_report_FuelReceptionsList_new_AV @gasStations=N'1,2,901,902',
    @startDate='2021-11-01 00:00:00',
    @endDate='2021-11-07 00:00:00',
    @fuelTypes=N'10,4,1,2,6,3,12,7,5,8,9,11',
    @receptionTypes=N'1', 
    @carDrivers='Безбородов Владимир Анатольевич'""")

    print(result_proc_db)
