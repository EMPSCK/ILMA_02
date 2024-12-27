male = open('male_names_rus.txt', "r", encoding="utf_8_sig")
female = open('female_names_rus.txt', "r", encoding="utf_8_sig")
male = male.readlines()
female = female.readlines()
for i in range(len(male)):
    male[i] = male[i][0:-1]

male = set(male)
print(male)