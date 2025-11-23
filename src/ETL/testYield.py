

def generator_con_1():
    yield "Từ con 1 - A"
    yield "Từ con 1 - B"

def generator_con_2():
    yield "Từ con 2 - X"
    yield "Từ con 2 - Y"

def generator_cha():
    print("Bắt đầu generator cha")
    yield from generator_con_2() # Ủy quyền cho generator_con_1
    print("Tiếp tục sau generator_con_1")
    yield from generator_con_1() # Ủy quyền cho generator_con_2
    print("Kết thúc generator cha")

print("--- Sử dụng yield from ---")
for val in generator_cha():
    print(val)