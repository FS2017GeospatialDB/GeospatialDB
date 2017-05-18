import math

earthDiameterkm = 12741.9811
earthDiametercm = earthDiameterkm * 1000 * 100
earthRadiuscm = earthDiametercm / 2
earthCircleAreacm = math.pi * math.pow(earthRadiuscm, 2)

print earthCircleAreacm

unit = ['cm2', 'dm2','1m2', 'xm2','hm2','km2']

for i in range(1,31):
    numSquare = math.pow(math.pow(2,i),2)
    coverArea = earthCircleAreacm / numSquare
    j=0
    while coverArea > 100:
        coverArea /= 100
        j = j+1
        if j == 5:
            break
    
    print 'level:\t', i , '\t', coverArea, unit[j]


# level:	1 	31878934.9231 km2
# level:	2 	7969733.73079 km2
# level:	3 	1992433.4327 km2
# level:	4 	498108.358174 km2
# level:	5 	124527.089544 km2
# level:	6 	31131.7723859 km2
# level:	7 	7782.94309647 km2
# level:	8 	1945.73577412 km2
# level:	9 	486.433943529 km2
# level:	10 	121.608485882 km2
# level:	11 	30.4021214706 km2
# level:	12 	7.60053036765 km2
# level:	13 	1.90013259191 km2
# level:	14 	47.5033147978 hm2
# level:	15 	11.8758286994 hm2
# level:	16 	2.96895717486 hm2
# level:	17 	74.2239293716 xm2
# level:	18 	18.5559823429 xm2
# level:	19 	4.63899558572 xm2
# level:	20 	1.15974889643 xm2
# level:	21 	28.9937224108 1m2
# level:	22 	7.24843060269 1m2
# level:	23 	1.81210765067 1m2
# level:	24 	45.3026912668 dm2
# level:	25 	11.3256728167 dm2
# level:	26 	2.83141820418 dm2
# level:	27 	70.7854551044 cm2
# level:	28 	17.6963637761 cm2
# level:	29 	4.42409094403 cm2
# level:	30 	1.10602273601 cm2
