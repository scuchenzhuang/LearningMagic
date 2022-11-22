



'''求a和b的最大公约数'''
def gcd(a,b):
    return a if b==0 else gcd(b,a%b)

'''求a和b的最小公倍数'''
def lcm(a,b):
    return a*b/gcd(a,b)
