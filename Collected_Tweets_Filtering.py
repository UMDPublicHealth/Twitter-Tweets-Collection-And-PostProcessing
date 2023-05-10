import argparse
import copy
import glob
import gzip
import json
import sys
import re
import os
import time
import csv
import datetime


# there are some limits when using key word to collect tweets.
# for example, a key word term "white boy" will match with the tweet "white shirt boy".
# so this file is used to filter out irrelevant tweets and also convert gz files to csv files.


# define path

# set key word list path
infile="E:/key_word_list.csv"
# define target files
Mon="*.gz"
indir = "E:/" + Mon
# set result file path
outfile= "E:/" + "result.csv"


# process keyword list
def processKeywords(infile):
    raceDict = {}
    with open(infile, 'r', encoding="utf-8") as g:
        print("Processing file: {}".format(infile))
        g.readline() #really should process with csv library. maybe later
        for line in g:
            pieces = [x.lower().strip() for x in line.split(",")]
            try:
                assert pieces[0] not in raceDict
            except AssertionError:
                print("{} is a duplicate".format(pieces[0]))
                continue
            if pieces[1]=="exclude":
                continue
            raceDict[pieces[0]] = {"Race":pieces[1]}
    assert " " not in raceDict
    print("raceDict has {} entries".format(len(raceDict)))
                
    return(raceDict)
 
 

# process tweets
# find and extract parameters based on needs
def processTweets(indir, outfile, raceDict):

    OUTFILE = open(outfile, "w", encoding='utf-8', newline='')
    
    labels = ["conversation_id", "tweet_id", "tweet_text", "tweet_timestamp", 
              "geo_coord", "place_id", "place_bbox", "place_full_name", 
              "place_type", "place_name", "entities_tweet_mention_username", 
              "entities_tweet_mention_id", "user_id", "user_place", "user_name", 
              "user_username", "user_create_at", "user_profile", "user_pinned_tweet_id", 
              "user_follower_count", "user_following_count", "user_tweets_count", 
              "user_listed_count", "user_verified", "tweets_in_reply_to_user_id", "tweets_language",
              "tweets_retweet_count", "tweets_reply_count", "tweets_like_count", 
              "tweets_quote_count", "tweets_referenced_type", "tweets_referenced_id", 
              "tweets_source", "racecat1", "racecat2", "racecat3", "raceterm1", 
              "raceterm2", "raceterm3" ]

    writer = csv.writer(OUTFILE)
    writer.writerow(labels)

    
    decodeErrors = 0
    valueErrors = 0
    typeKeyErrors = 0
    noErrors = 0
    totalFinds = 0

    raceList = []
    racePairList = []
    
    
    for k in raceDict:
        if k.startswith("#"):
            s = r'%s\b' % k #for hashtags, just check boundary on right
            raceList.append(s)
        elif "+" not in k:
            s = r'\b%s\b' % k
            raceList.append(s)
        else:
            pieces = [x.strip() for x in k.split("+")]
            piece1 = r'\b%s\b' % pieces[0]
            piece2 = r'\b%s\b' % pieces[1]
            racePairList.append([piece1, piece2])
    raceRegexS = "|".join(raceList)
    raceBigRegex = re.compile(raceRegexS)


            
    for fname in glob.glob(indir):
        
        print("Working on file: {}".format(fname))
        print(datetime.datetime.now())
        if not fname.endswith(".gz"):
            print("I expect all files to end with .gz. Skipping file {}".format(fname))
            continue
        for line in gzip.open(fname):
            
            if not line:
                continue

            try:
                
                line = line.decode('utf-8').strip()
                
                
                
            except UnicodeDecodeError:
                decodeErrors +=1
                continue
            try:

                tweet_obj = json.loads(line, encoding='utf-8')
                
            except ValueError:
                # Skip lines that aren't JSON objects
                # e.g., '[WARN]' lines
                valueErrors +=1
                continue
            
            
            for i in range(len(tweet_obj['data'])):        
                #noErrors count number of general tweets collected
                noErrors +=1
                #get tweet text
                origTweet = tweet_obj['data'][i]['text']
                
                #get rid of troublesome white space
                origTweet = origTweet.replace("\t", " ")
                origTweet = origTweet.replace("\n", " ")
                origTweet = origTweet.replace("\r", " ")
                origTweet = re.sub(r"\s+", ' ', origTweet)
    
                tweet = origTweet.lower() #use this for searching but keep orig
    
             
                #hopefully this speeds things up because it's one search instead of 500+ for most things
                racePair = False
                for pair in racePairList:
                    n = re.search(pair[0], tweet)
                    p = re.search(pair[1], tweet)
                    if n is not None and p is not None:
                        racePair = True
                        break
                m = re.search(raceBigRegex, tweet)
                if m is None and racePair==False:
                    continue
    
                finds = [] #all matches
                for k in raceDict: #search for every term in the dictionary
                    if k.startswith("#"):
                        searchTerm = r'%s\b' % k #just check for boundary on right
                        m = re.finditer(searchTerm, tweet)
                        for match in m:
                            finds.append(match.span())#keep index in sentence of finds
                    elif "+" not in k: # '+' search terms are different
                        searchTerm = r'\b%s\b' % k
                        m = re.finditer(searchTerm, tweet)
                        for match in m:
                            finds.append(match.span()) #keep index in sentence of finds
    
                    else:
                        search1, search2 = [x.strip() for x in k.split("+")] #eg tamir + rice
                        searchTerm1 = r'\b%s\b' % search1
                        searchTerm2 = r'\b%s\b' % search2
                        m = re.search(searchTerm1, tweet)
                        n = re.search(searchTerm2, tweet)
                        if m is not None and n is not None:
                           finds.append(k) 
    
                #warning, don't forget, maybe change in future, variable finds can
                #contain 2 types of data, a span e.g. (4, 10) or a string e.g.
                #"tamir + rice"!!!
                if len(finds)==0:
                    continue #no matches, continue to next Tweet
                if len(finds)>3:
                    continue #ignoring those that have more than 3 race terms

                
                info = []
                #1. conversationID
                try:
                    info.append(str(tweet_obj['data'][i]['conversation_id'])+'g')
                except:
                    convs_id = ''
                    info.append(convs_id)
                #2. tweetID
                info.append(str(tweet_obj['data'][i]['id'])+'g')
                #3. tweet text
                info.append(origTweet)
                #4. timestamp
                info.append(tweet_obj['data'][i]["created_at"])
                #5. lat/long
                latlon = ''
                if "geo" in tweet_obj['data'][i].keys():
                    if "coordinates" in tweet_obj['data'][i]['geo'].keys():
                        if str(tweet_obj['data'][i]['geo']['coordinates']['type']) == "Point":
                                latlon = tweet_obj['data'][i]['geo']['coordinates']['coordinates']
                    else:
                        latlon = ''
                else:
                    latlon = ''
                        
                info.append(str(latlon))
    
           
                
                #place field (about tweet)
                
                #6. place id
                #7. bbox
                #8. full name
                #9. place type 
                #10. place name
                #11. entities tweet mention username
                #12. entites tweet mention id
                bb = ''
                ptl = ''
                pty = ''
                pname = ''
                p_id = ''
                tm = ''
                tmid = ''
                
                if 'geo' in tweet_obj['data'][i].keys():
                    if 'place_id' in tweet_obj['data'][i]['geo'].keys():
                        p_id = tweet_obj['data'][i]['geo']['place_id']
                        
                        
                        for p in range(len(tweet_obj['includes']['places'])):
                            if tweet_obj['includes']['places'][p]['id'] == p_id:
                                if 'geo' in tweet_obj['includes']['places'][p].keys():
                                    if str(tweet_obj['includes']['places'][p]['geo']['type']) == "Feature":
                                        bb = tweet_obj['includes']['places'][p]['geo']['bbox']
                                        
                                if 'full_name' in tweet_obj['includes']['places'][p].keys():
                                    ptl = tweet_obj['includes']['places'][p]['full_name'].replace(",", ";")
                                if 'place_type' in tweet_obj['includes']['places'][p].keys():
                                    pty = tweet_obj['includes']['places'][p]['place_type']
                                if 'name' in tweet_obj['includes']['places'][p].keys():
                                    pname = tweet_obj['includes']['places'][p]['name']
                                
                                break
                
                
                if 'entities' in tweet_obj['data'][i].keys():
                    if 'mentions' in str(tweet_obj['data'][i]['entities'].keys()):
                        if 'username' in str(tweet_obj['data'][i]['entities']['mentions'][0].keys()):
                            tm = str(tweet_obj['data'][i]['entities']['mentions'][0]['username'])
                        if 'id' in str(tweet_obj['data'][i]['entities']['mentions'][0].keys()):
                            tmid = str(tweet_obj['data'][i]['entities']['mentions'][0]['id']) + "g"
                
                
                info.append(str(p_id))
                info.append(str(bb))
                info.append(str(ptl))
                info.append(str(pty))
                info.append(str(pname))
                info.append(str(tm))
                info.append(str(tmid))
                
                # user field

                pal = ''
                us = ''
                nf = ''
                nam = ''
                crt = ''
                descp = ''
                ptid = ''
                nfing = ''
                nt = ''
                nl = ''
                verif = ''
                descp_clean = ''
                nam_clean = ''
                pal_clean = ''
                a_id = ''
                
                if 'author_id' in tweet_obj['data'][i].keys():
                    a_id = tweet_obj['data'][i]['author_id']
                    

                    
                    for a in range(len(tweet_obj['includes']['users'])):
                        if tweet_obj['includes']['users'][a]['id'] == a_id:
                            if 'location' in tweet_obj['includes']['users'][a].keys():
                                pal = str(tweet_obj['includes']['users'][a]['location'].replace(",", ";"))
                                pal_clean = re.sub(r"\s+", ' ', pal)
                            if 'pinned_tweet_id' in tweet_obj['includes']['users'][a].keys():
                                ptid = str(tweet_obj['includes']['users'][a]['pinned_tweet_id']) + "g"
                            
                            try:
                                nam = tweet_obj['includes']['users'][a]['name']
                                nam_clean = re.sub(r"\s+", ' ', nam)
                            except:
                                nam_clean = ''
                            try:
                                us = tweet_obj['includes']['users'][a]['username']
                            except:
                                us = ''
                            try:
                                crt = tweet_obj['includes']['users'][a]['created_at']
                            except:
                                crt = ''
                            try:
                                descp = tweet_obj['includes']['users'][a]['description']
                                descp_clean = re.sub(r"\s+", ' ', descp)
                            except:
                                descp_clean = ''
                            try:
                                nf = tweet_obj['includes']['users'][a]['public_metrics']['followers_count']
                            except:
                                nf = ''
                            try:
                                nfing = tweet_obj['includes']['users'][a]['public_metrics']['following_count']
                            except:
                                nfing = ''
                            try:
                                nt = tweet_obj['includes']['users'][a]['public_metrics']['tweet_count']
                            except:
                                nt = ''
                            try:
                                nl = tweet_obj['includes']['users'][a]['public_metrics']['listed_count']
                            except:
                                nl = ''
                            try:
                                verif = tweet_obj['includes']['users'][a]['verified']
                            except:
                                verif = ''
    
    
                            break                
                #13. author id
                #14. place_account_level
                #15. name
                #16. username
                #17. create at
                #18. description
                #19. pinned tweet id
                #20. number of followers
                #21. number of following
                #22. number of tweets
                #23. number of listed
                #24. user is verified or not
                info.append(str(a_id)+'g')
                info.append(str(pal_clean))
                info.append(str(nam_clean))
                info.append(str(us))
                info.append(str(crt))
                info.append(str(descp_clean))
                info.append(str(ptid))
                info.append(str(nf))
                info.append(str(nfing))
                info.append(str(nt))
                info.append(str(nl))
                info.append(str(verif))
                

                #25. tweet: in reply to user id
                #26. tweet language
                #27. tweet retweet count
                #28. tweet reply count
                #29. tweet like count
                #30. tweet quote count
                #31. referenced_tweets type 
                #32. referenced_tweets id 
                #33. source
                
                irtuid = ''
                tl = ''
                retc = ''
                repc = ''
                likc = ''
                quotc = ''
                rftt = ''
                rftid = ''
                src = ''
                
                
                if 'lang' in tweet_obj['data'][i].keys():
                    tl = str(tweet_obj['data'][i]['lang'])
                
                if 'source' in tweet_obj['data'][i].keys():
                    src = str(tweet_obj['data'][i]['source'])
                
                if 'referenced_tweets' in tweet_obj['data'][i].keys():
                    try:
                        rftt = str(tweet_obj['data'][i]['referenced_tweets'][0]['type'])
                    except:
                        rftt = str('')
                    try:    
                        rftid = str(tweet_obj['data'][i]['referenced_tweets'][0]['id']) + "g"
                    except:
                        rftid = str('')

                if 'in_reply_to_user_id' in tweet_obj['data'][i].keys():
                    irtuid = str(tweet_obj['data'][i]['in_reply_to_user_id']) + "g"
               

                if "public_metrics" in tweet_obj['data'][i].keys():
                    try:
                        retc = str(tweet_obj['data'][i]['public_metrics']['retweet_count'])
                    except:
                        retc = ''
                    try:
                        repc = str(tweet_obj['data'][i]['public_metrics']['reply_count'])
                    except:
                        repc = ''
                    try:
                        likc = str(tweet_obj['data'][i]['public_metrics']['like_count'])
                    except:
                        likc = ''
                    try:
                        quotc = str(tweet_obj['data'][i]['public_metrics']['quote_count'])
                    except:
                        quotc = ''                

                info.append(str(irtuid))
                info.append(str(tl))
                info.append(str(retc))
                info.append(str(repc))
                info.append(str(likc))
                info.append(str(quotc))
                info.append(str(rftt))
                info.append(str(rftid))
                info.append(str(src))
                
    
                if len(finds)==1: #no need to check substring matches
                    pass
    
                else: #remove matches that are proper subsets of other matches
                    trueFinds = copy.deepcopy(finds)
                    for find1 in finds:
                        if type(find1)==str:
                            continue
                        for find2 in finds:
                            if type(find2[0])==str:
                                continue
                            if find1 != find2 and find1[0]>=find2[0] and find1[1]<=find2[1]:
                                try:
                                    trueFinds.remove(find1)
                                except:
                                    print("couldn't remove: {}".format(find1))
                                    continue
                    finds = trueFinds
                    
                totalFinds += len(finds)
                #negativeSum = 0
                raceCategories = []
                raceTerms = []
                #order is negativesum, then all race categories, then all race terms
    
                for find in finds: #tuples represent start, end point in str
                    if type(find[0])==int:#from continuous string searches
                        s = tweet[find[0]:find[1]]
                    else:
                        s = find #from e.g. "tamir + rice"
                    #print("found: {}".format(s))
                    #negativeSum = negativeSum + raceDict[s]["Negative"]
                    raceCategories.append(raceDict[s]["Race"])
                    raceTerms.append(s)
                #but even if there are fewer than 3 race terms, we must pad this list with empty string
                while len(raceCategories)<3:
                    raceCategories.append('')
                while len(raceTerms)<3:
                    raceTerms.append('')
                    
                #info.append(str(negativeSum))
                #34. race cate 1
                #35. race cate 2
                #36. race cate 3
                #37. race term 1
                #38. race term 2
                #39. race term 3
                info.extend(raceCategories)
                info.extend(raceTerms)
                
                

                writer.writerow(info) #\t
    
                if totalFinds%100==0:
                    OUTFILE.flush()
    counts="Finished. Decode errors: {}; Value errors: {}; Type or Key errors = {}; No errors: {}".format(decodeErrors, valueErrors, typeKeyErrors, noErrors)
    print(counts)            
    OUTFILE.close()
    return(counts)
    
 
    

if __name__ == "__main__":
    time0=time.time()

    raceDict = processKeywords(infile)  

    processTweets(indir, outfile, raceDict)
    time1=time.time()
    process_time=time1-time0
    print (process_time)
    


# code execution looks like:
# Finished. Decode errors: 0; Value errors: 0; Type or Key errors = 156; No errors: 1621660
# 250.12740087509155
# Finished. Decode errors: 0; Value errors: 0; Type or Key errors = 156; No errors: 1621660
# 2505.21435213089