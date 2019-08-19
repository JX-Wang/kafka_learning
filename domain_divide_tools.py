# usr/bin/enc python
# encoing:utf-8
"""
for dividing domain
===================
Author @ wangjunxiong
Date @ 2019/7/20
"""
from kafka_producer import kafka_producer


def divide(listTemp, n):
    for i in range(0, len(listTemp), n):
        yield listTemp[i:i+n]


class domain_divide(object):
    """:param blocks, id, type
       :return divide domain list with id and type appended
    """
    def __init__(self, blocks, id, type):
        if not id:
            self.id = -1
        else:
            self.id = id
        self.blocks = blocks
        self.type = type

    def bomb(self, value):
        domain_rst = []
        block_len = len(value) / self.blocks
        if len(value) < self.blocks:
            return "Error Can Divide blocks, Maybe blocks > values"
        # print block_len
        try:
            domain_rst = [value[i:i+block_len] for i in range(0, len(value), block_len)]
            if len(value) % self.blocks != 0:
                last = domain_rst.pop()
                domain_rst[-1].extend(last)
            else:
                pass
            # domain_rst = divide(value, self.blocks)
            # print type(domain_rst)
            for domain_list in domain_rst:
                domain_list.append(self.id)
                domain_list.append(self.type)
        except Exception as e:
            print "E Domain Divide Error ->", str(e)
        return domain_rst


if __name__ == '__main__':
    file_name = "b_sec_11011111111"
    with open(file_name, 'r') as f:
        # domains = f.readlines()
        # print domains
        domains = ['A.com', 'B.com', 'C.com', 'D.com', 'E.com', 'F.com', 'G.com', 'H.com', 'I.com', 'J.com', 'K.com', 'S.com']
        rst = domain_divide(id=1, type="query", blocks=5).bomb(domains)
        # print type(rst), rst  # next(rst)
        for domain in domains:
            kafka_producer().push(domain)

        """
        input
        domains = ['A.com', 'B.com', 'C.com', 'D.com', 'E.com', 'F.com', 'G.com', 'H.com', 'I.com', 'J.com', 'K.com', 'S.com']
        rst = domain_divide(id=1, type="query", blocks=4).bomb(domains)
        
        output 
        ['A.com', 'B.com', 'C.com', 1, 'query']
        ['D.com', 'E.com', 'F.com', 1, 'query']
        ['G.com', 'H.com', 'I.com', 1, 'query']
        ['J.com', 'K.com', 'S.com', 1, 'query']
        """


