/*
  -- Dave Skura, 2022
*/

SELECT postalcode
    ,RANK() OVER (PARTITION BY FSA ORDER BY latitude) as rnk
    ,LAG(latitude,1) OVER (ORDER BY longitude) as lag1
    ,LEAD(latitude,1) OVER (ORDER BY longitude) as lead1
   
    ,fsa, latitude, longitude, place, fsa1, fsaprovince, areatype 
FROM postal_codes
ORDER BY longitude;
