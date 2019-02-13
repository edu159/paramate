# @list_generator_dynamic
# @list_generator_static

def gen_scalar_var(func):
    def gen_scalar_var_f(instance):
        return func(instance)
    return gen_scalar_var_f 

def gen_scalar_const(func):
    def gen_scalar_const_f(instance):
        return func(instance)
    return gen_scalar_const_f 

def gen_list_const(func):
    def gen_list_const_f(params, length):
        l = func(params, length)
        if length != len(l):
            raise Exception("Length of list different {} != {}.".format(length, len(l)))
        else:
            return l
    return gen_list_const_f
