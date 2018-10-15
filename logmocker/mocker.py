import random
from faker import Factory

fake = Factory().create('zh_CN')


class Mocker:
    def __init__(self):
        self.imei = self.random_imei()

    def random_imei(self):
        return ""

    def random_phone_number(self):
        """随机手机号"""
        return fake.phone_number()

    def random_name(self):
        """随机姓名"""
        return fake.name()

    def random_address(self):
        """随机地址"""
        return fake.address()

    def random_email(self):
        """随机email"""
        return fake.email()

    def random_ipv4(self):
        """随机IPV4地址"""
        return fake.ipv4()

    def random_str(min_chars=0, max_chars=8):
        """长度在最大值与最小值之间的随机字符串"""
        return fake.pystr(min_chars=min_chars, max_chars=max_chars)

    def factory_generate_ids(starting_id=1, increment=1):
        """ 返回一个生成器函数，调用这个函数产生生成器，从starting_id开始，步长为increment。 """

        def generate_started_ids():
            val = starting_id
            local_increment = increment
            while True:
                yield val
                val += local_increment

        return generate_started_ids

    def factory_choice_generator(values):
        """ 返回一个生成器函数，调用这个函数产生生成器，从给定的list中随机取一项。 """

        def choice_generator():
            my_list = list(values)
            rand = random.Random()
            while True:
                yield random.choice(my_list)

        return choice_generator


if __name__ == '__main__':
    mocker = Mocker()
    print(mocker.random_phone_number())
    print(mocker.random_name())
    print(mocker.random_address())
    print(mocker.random_email())
    print(mocker.random_ipv4())
    print(mocker.random_str(min_chars=6, max_chars=8))
    id_gen = mocker.factory_generate_ids(starting_id=0, increment=2)()
    for i in range(5):
        print(next(id_gen))

    choices = ['John', 'Sam', 'Lily', 'Rose']
    choice_gen = mocker.factory_choice_generator(choices)()
    for i in range(5):
        print(next(choice_gen))
