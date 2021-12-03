import random

def estimate_pi(n):
    num_circle = 0
    num_total = 0
    for _ in range(n):
       x = random.uniform(0, 1)
       y = random.uniform(0, 1)
       distance = x**2 + y**2
       if distance <= 1:
           num_circle +=1
       num_total +=1
    return 4 * num_circle/num_total

print(estimate_pi(1000))
 
 #Joost test