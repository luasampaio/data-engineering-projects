print(f"Session {'Luciana'}: User Input")

favorite_color = input("What is your favorite color: ")
print(f"Your favorite color is: {favorite_color}")

year_of_birth = int(input("What year were you born: "))
print(f"Your birth year is: {year_of_birth}")

you_like_python = input("Do you like Python so far (yes/no): ").strip().lower() == "yes"
print(f"You {'like' if you_like_python else 'dislike'} Python so far")

vacation_budget = float(input("What's your vacation budget: "))
print(f"Your total vacation budget is: ${vacation_budget:.2f}")
print(f"Your total vacation budget is: ${'{:.2f}'.format(vacation_budget)}")
print(f"Your total vacation budget is: ${round(vacation_budget, 2)}")

numbers = input("Please enter two numbers: ").split()
sum_of_numbers = int(numbers[0]) + int(numbers[1])
print(f"{numbers[0]} + {numbers[1]} = {sum_of_numbers}")
print(numbers[0] + " + " + numbers[1] + " = " + str(sum_of_numbers))
print(str(numbers[0]) + " + " + str(numbers[1]) + " = " + str(sum_of_numbers))

temp_in_celsius     = float(input("What is the temperature in Celsius: "))
temp_in_fahrenheit  = (temp_in_celsius * 9 / 5) + 32
print(f"The temperature in Celsius is: {temp_in_celsius:.1f}")
print(f"The temperature in Fahrenheit is: {temp_in_fahrenheit:.1f}")