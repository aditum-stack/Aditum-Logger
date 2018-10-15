"""
模拟数据对象
"""


class Info_Tusi:

    def __init__(self):
        self.info_tusi_identity = self.random_identity()

    def random_identity(self, phone="18888888888", imei="116789ABCDEF0123456789ABCDEF11"):
        """
        生成随机身份校验请求
        """
        time = "todo..."
        type = "todo..."
        info = "身份校验...phone:" + phone + ",imei:" + imei
        return time + " " + type + " " + info
