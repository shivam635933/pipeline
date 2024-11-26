
class MetaClassOld(type):
    STORE={}
    def __new__(cls, clsname, *args, **kwargs):
        key = f"{clsname.lower()}_{'-'.join(list(args))}"
        
        if key in MetaClassOld.STORE:
            print("Returned from storage")
            return MetaClassOld.STORE[key]

        cl = type(clsname, args, kwargs)
        if len(args) != 0:
            print("created new pipeline from object")
            print(args)
            MetaClassOld.STORE[key] = cl
        return cl 
    



class MetaClass(type): 
    # Inherit from "type" in order to gain access to method __call__
    def __init__(self, *args, **kwargs):
        self.__instance = {} # Create a variable to store the object reference
        super().__init__(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        if args in self.__instance:
            return self.__instance[args]
        self.__instance[args] = super().__call__(*args, **kwargs) # Call the __init__ method of the subclass (Spam) and save the reference
        return self.__instance[args]
        
