import math


class Point:
    def __init__(self, x, y):
        self.x = x
        self.y = y

    def get_distance(self, point):
        return math.sqrt(self.get_distance2(point))

    def get_distance2(self, point):
        return (self.x - point.x) ** 2 + (self.y - point.y) ** 2

    @property
    def angle(self):
        return self.get_relative_angle(Point(0, 0))

    def get_relative_angle(self, point):
        """
        获取输入点相对于当前点的x轴轴角度
        x轴：16000 units wide  y轴：9000 units high
        X=0, Y=0 is the top left pixel
        An angle of 0 corresponds to facing east, 90 is south,
        180 west and 270 north
        :param point: 指向的目标点，当前点位起点
        :return:
        """
        if self == point:
            return 0

        d = self.get_distance(point)
        dx = (point.x - self.x) / d

        angle = math.acos(dx) * 180 / math.pi

        if self.y < point.y:
            angle = 360 - angle

        return angle

    def __eq__(self, other):
        return self.x == other.x and self.y == other.y

    def __repr__(self):
        return "({}, {})".format(self.x, self.y)


class Unit(Point):
    def __init__(self, x, y, radius):
        super().__init__(x, y)
        self.radius = radius

    def collision(self, unit):
        pass

    def bounce(self, unit):
        pass


class Pod(Unit):
    def __init__(self, x, y, checkpoint, radius=400):
        super().__init__(x, y, radius)
        self.checkpoint = checkpoint
        self.radius = radius

        # 假设开始就朝向目标
        self.orient = self.get_relative_angle(checkpoint)
        self.checkpoint_angle = 0

        # 速度分解
        self.vx = 0
        self.vy = 0

        self.timeout = 100

    # def set_orient(self, checkpoint_angle):
    #     # 如果有checkpoint_angle 输入
    #     self.checkpoint_angle = checkpoint_angle
    #     self.orient = (self.get_relative_angle(self.checkpoint) + 180 - checkpoint_angle) % 360

    def set_checkpoint(self, checkpoint):
        # 改变目标后，pod的方向orient不会变，需要修改checkpoint_angle
        self.checkpoint = checkpoint
        self.checkpoint_angle = self.cal_checkpoint_angle()

    def cal_checkpoint_angle(self):
        angle = self.get_relative_angle(self.checkpoint)

        # 统一顺时针（往右）转动为正值，往左转是负值，返回值为 [-180， 180]
        right = angle - self.orient if self.orient <= angle else 360 - self.orient + angle
        left = self.orient - angle if self.orient >= angle else self.orient + 360 - angle

        if right < left:
            return right
        else:
            return -left

    def rotate(self):
        """
        最大转向角度为+-18度，实际达不到，可能跟thrust有关系
        :return:
        """
        if self.checkpoint_angle > 18:
            delta_angle = 18
        elif self.checkpoint_angle < -18:
            delta_angle = -18
        else:
            delta_angle = self.checkpoint_angle

        self.orient += delta_angle

        if self.orient >= 360:
            self.orient -= 360
        elif self.orient < 0:
            self.orient += 360

    def cal_velocity(self, thrust):
        """
        速度有一个上限
        :param thrust:
        :return:
        """
        radian = self.orient * math.pi / 180

        self.vx += math.cos(radian) * thrust
        self.vy += math.sin(radian) * thrust

    def move(self, turn=1):
        """
        如果有碰撞，turn设置为0.5, 一半为一个turn的移动
        :param turn:
        :return:
        """
        self.x += self.vx * turn
        self.y += self.vy * turn

    def end(self):
        """
        Once the pod has moved we need to apply friction and round (or truncate) the values.
        :return:
        """
        self.x = round(self.x)
        self.y = round(self.y)
        
        # 摩擦系数 0.85
        friction = 0.85
        self.vx = int(self.vx * friction)
        self.vy = int(self.vy * friction)

        # Don't forget that the timeout goes down by 1 each turn.
        # It is reset to 100 when you pass a checkpoint
        self.timeout -= 1

    def simulate_a_turn(self, thrust):
        self.rotate()
        self.cal_velocity(thrust)
        self.move()
        self.end()


if __name__ == "__main__":
    # from src.codersstrikeback import Unit
    mypos = Point(10705, 5691)
    target = Point(3571, 5202)
