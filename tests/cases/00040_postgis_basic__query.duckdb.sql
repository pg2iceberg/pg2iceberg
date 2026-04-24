.nullvalue '\N'
SELECT
    id,
    name,
    upper(hex(pt)) AS pt_hex,
    ST_AsText(ST_GeomFromHEXEWKB(upper(hex(pt)))) AS pt_wkt,
    -- Spatial predicate: is the point within the western hemisphere bbox?
    ST_Within(
        ST_GeomFromHEXEWKB(upper(hex(pt))),
        ST_GeomFromText('POLYGON((-180 -90, -30 -90, -30 90, -180 90, -180 -90))')
    ) AS in_western_hem,
    -- Spatial measurement: planar distance from (0, 0).
    round(ST_Distance(
        ST_GeomFromHEXEWKB(upper(hex(pt))),
        ST_GeomFromText('POINT(0 0)')
    ), 4) AS dist_from_null_island,
    -- Verify the geography column round-tripped identically to the geometry one.
    upper(hex(loc)) = upper(hex(pt)) AS loc_matches_pt
FROM t
ORDER BY id;
