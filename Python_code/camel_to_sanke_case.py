# def camel_to_snake_case(camel_str):
#     snake_str = camel_str[0].lower()  # Convert first character to lowercase
#     for char in camel_str[1:]:  # Iterate through remaining characters
#         if char.isupper():  # If the character is uppercase
#             snake_str += '_' + char.lower()  # Add an underscore and its lowercase version
#         else:
#             snake_str += char  # Otherwise, just add the character as it is
#     return snake_str
#
# # Test cases
# print(camel_to_snake_case("CamelCaseString"))

def camel_to_snake_case(camel_str):
    snake_str=camel_str[0].lower()
    for char in camel_str[1:]:
        if char.isupper():
            snake_str+='_' + char.lower()
        else:
            snake_str+=char
    return snake_str
print(camel_to_snake_case('CamelCase'))



def camel_to_snake(column_name):
    snake_str=column_name[0].lower()
    for char in column_name[1:]:
        if char.isupper():
            snake_str+='_'+char.lower()
        else:
            snake_str+=char
    return snake_str

print(camel_to_snake('CustomerId'))