def generateMonthlyPathList(year, month, day):
   for d in range(1, int(day) + 1): 
        print(f"https://importantdata@location/{year}/{month}/{d}/")


generateMonthlyPathList("2021", "05", "17")
