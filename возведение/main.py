def power(a, n):
    if n == 0:
        return 1
    if n % 2 == 0:
        return power(a, n / 2) ** 2
    else:
        return a*power(a, n - 1)
a = int(input())
n = int(input())
print(power(a, n))