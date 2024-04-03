# sum of elemets in nested list
a=[[1,2],[3,4]]
list=[]
for i in a:
    sum = 0
    for j in i:
        sum=sum+j
    list.append(sum)

print(list)