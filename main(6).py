import os.path
import re
from mpi4py import MPI
from collections import Counter
import pandas as pd
import json
import time

def can_find_in_sal(placeFullName,salfile):
    can_find = False
    checkGccValue = ""
    if "," in placeFullName:
        if placeFullName.split(",")[0] in salfile:
            gccValue = salfile[placeFullName.split(",")[0]]["gcc"]
            if gccValue == "1gsyd" or gccValue == "2gmel" or gccValue == "3gbri" or gccValue == "4gade" or gccValue == "5gper" or gccValue == "6ghob" or gccValue == "7gdar" or gccValue == "8acte" or gccValue == "9oter":
                can_find = True
                checkGccValue = gccValue
    if "," not in placeFullName:
        if placeFullName in salfile:
            gccValue = salfile[placeFullName]["gcc"]
            if gccValue == "1gsyd" or gccValue == "2gmel" or gccValue == "3gbri" or gccValue == "4gade" or gccValue == "5gper" or gccValue == "6ghob" or gccValue == "7gdar" or gccValue == "8acte"or gccValue == "9oter":
                can_find = True
                checkGccValue = gccValue
    return can_find, checkGccValue


startRunningTime = time.time()


comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

blockSize = os.path.getsize("bigTwitter.json")//size
start = rank*blockSize
end = start+blockSize


with open("bigTwitter.json","r", encoding = 'UTF-8') as f:
    f.seek(start)

    totalData = []
    start_time = time.time()
    currentPosition = f.tell()
    while currentPosition <= end:
        line = f.readline()
        if line == "]\n":
            break
        if line == "  {\n":
            author = ""
            while 1:
                line = f.readline()
                if line in ["  },\n", "  }\n"]:
                    currentPosition = f.tell()
                    break
                if "author_id" in line :
                    authorID = line
                if "full_name" in line :
                    placeFullName = line
                    totalData.append(authorID+"^"+placeFullName)
                    



    print("while loop ends: ", time.time() - start_time)

totalDatas =  comm.gather(totalData, root = 0)


if rank == 0:

    
    salPath = "sal.json"
    with open(salPath) as sal:
        salfile = json.load(sal)

    start_total_dict_time = time.time()

    data = []
    for list in totalDatas:
        data = data+list
    results = []
    authorDict = {}
    for value in data:
        author = value.split("^")[0].split(":")[1].strip().replace("\"", "").replace(",", "")
        place = value.split("^")[1].split(":")[1].strip().replace("\"", "").lower()[:-1]
        results.append(author)
        if author not in authorDict.keys():
            canFindGcc, CheckGccValue = can_find_in_sal(place, salfile)
            if(canFindGcc):
                authorDict[author] = [CheckGccValue]
        else:
            canFindGcc, CheckGccValue = can_find_in_sal(place, salfile)
            if (canFindGcc):
                new_list = authorDict[author] + [CheckGccValue]
                authorDict[author] = new_list
        

    print("total_dict ends: ", time.time() - start_total_dict_time)

    # table 1
    start_T1_time = time.time()

    totalGccDict={}
    for value in authorDict.values():
        for element in value:
            if element not in totalGccDict.keys():
                totalGccDict[element] = 1
            else:
                totalGccDict[element] = totalGccDict[element] +1
    sortedTotalGccDict = sorted(totalGccDict, key=totalGccDict.get, reverse=True)
    updatedSortedTotalGccDict = []
    for area in sortedTotalGccDict:
        if area == "1gsyd":
            updatedSortedTotalGccDict.append(area+" (Greater Sydney)")
        if area == "2gmel":
            updatedSortedTotalGccDict.append(area+" (Greater Melbourne)")
        if area == "3gbri":
            updatedSortedTotalGccDict.append(area+" (Greater Brisbane)")
        if area == "4gade":
            updatedSortedTotalGccDict.append(area+" (Greater Adelaide)")
        if area == "5gper":
            updatedSortedTotalGccDict.append(area+" (Greater Perth)")
        if area == "6ghob":
            updatedSortedTotalGccDict.append(area+" (Greater Hobart)")
        if area == "7gdar":
            updatedSortedTotalGccDict.append(area+" (Greater Darwin)")
        if area == "8acte":
            updatedSortedTotalGccDict.append(area+" (Greater ACT)")
        if area == "9oter":
            updatedSortedTotalGccDict.append(area+" (Greater Other)")

    totalGccList = [totalGccDict[i] for i in sortedTotalGccDict]
    gccDataDice = {'Greater Capital City': updatedSortedTotalGccDict, 'Number of Tweets Made': totalGccList}
    gccDataFrame = pd.DataFrame.from_dict(gccDataDice)
    
    print("T1 ends: ", time.time() - start_T1_time)

    # table 2
    start_T2_time = time.time()

    collection = Counter(results)
    mostCounts = collection.most_common(10)
    dataFrame = pd.DataFrame(mostCounts, columns=['Author Id', 'Number of Tweets Made'])
#    dataFrame['Rank'] = range(1, len(mostCounts) + 1)
    dataFrame = dataFrame[['Author Id', 'Number of Tweets Made']]

    print("T2 ends: ", time.time() - start_T2_time)

    # table 3
    start_T3_time = time.time()
    authorAndCities = {}
    for key in authorDict.keys():
        resetList = authorDict[key]
        resetCapitalCityCollection = Counter(resetList)
        totalCities = len(resetCapitalCityCollection)
        authorAndCities[key] = totalCities

    top10AuthorAndCities = sorted(authorAndCities.items(), key=lambda x: x[1], reverse=True)
    top10AuthorID = top10AuthorAndCities[0:10]

    rankIDandTweet = 1
    IDList =[]
  #  Rank_list =[]
    Number_of_Unique_City_Locations_and_Tweets_list = []
    for tuple in top10AuthorID:
 #       Rank_list.append(rankIDandTweet)

        id = tuple[0]
        IDList.append(id)
        resetList = authorDict[id]
        resetCapitalCityCollection = Counter(resetList)
        totalCities = tuple[1]
        totalTweets = sum(resetCapitalCityCollection.values())
        numberofUniqueCityLocation = ""
        for keyid, valuenumber in resetCapitalCityCollection.items():
            newNumberandGcc = str(valuenumber) + re.split(r"\d", keyid)[1]
            numberofUniqueCityLocation = numberofUniqueCityLocation + newNumberandGcc +", "
        Number_of_Unique_City_Locations_and_Tweets = str(totalCities) + "(#" + str(totalTweets) + "tweets - " + numberofUniqueCityLocation + ")"
        Number_of_Unique_City_Locations_and_Tweets_list.append(Number_of_Unique_City_Locations_and_Tweets)

        rankIDandTweet = rankIDandTweet + 1

    final_dict = {"Author Id": IDList, "Number of Unique City Locations and #Tweets":Number_of_Unique_City_Locations_and_Tweets_list}
    table3DataFrame = pd.DataFrame.from_dict(final_dict)
    print("T3 ends: ", time.time() - start_T3_time)



    print(gccDataFrame)
    print('\n')

    print(dataFrame)
    print('\n')

    print(table3DataFrame)
    print('\n')


    gccDataFrame.to_csv("result1.csv", index=False, header = "count the number of tweets in the various capital cities", sep='\t')
    dataFrame.to_csv("result2.csv",mode = 'a', index=False, header = "count the number of tweets made by the same individual and return the top 10 tweeters", sep='\t')
    table3DataFrame.to_csv("result3.csv", mode = 'a',index=False, header = " those tweeters that have tweeted in the most Greater Capital cities and the number of times they have tweeted from those locations", sep='\t')

    endTunningTime = time.time()
    totalRunningTime = endTunningTime - startRunningTime
    print('Total Running time: ' + str(round(totalRunningTime, 5)) + ' sec\n')
    