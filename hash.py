import math

# key:          the 10 bit ASCII key (ASCII characters in the range 32 - 126 (' ' to '~'))
# charIndex:    the index of the character in the key that is being looked at
# bucketCount:  the number of buckets the current key could be distributed to
# startBucket:  the index of the first bucket in which the key could go

#Returns the index of the bucket/node that a key should be distributed to
def bucketHash(key, charIndex, bucketCount, startBucket):
    #Calculate the size of the character range for each bucket (95 possible ASCII characters)
    bucketRange = 95.0 / bucketCount
    #Get the index of the bucket the key must go into based on the current char index
    #(minus 32 because our first key has an ASCII value of 32)
    bucketIndex = math.floor((ord(key[charIndex]) - 32.0) / bucketRange)

    #If there is more than 1 bucket for the current key based on the current char index
    if (bucketRange < 1):
        #Determine how many buckets there are available for the current ASCII character
        curCharBucketCount = math.ceil(bucketCount / 95.0)

        #We must move onto the next character in the key to determine which bucket this key goes in
        newCharIndex = charIndex + 1
        return bucketIndex + bucketHash(key, newCharIndex, curCharBucketCount, bucketIndex)
    else:
        return bucketIndex
