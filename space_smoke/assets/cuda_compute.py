import ctypes as c
import json

cuda = c.CDLL("libcuda.so.1")


def api(name, args):
    f = getattr(cuda, name)
    f.argtypes = args
    f.restype = c.c_int

    def call(*values):
        rc = f(*values)
        if rc:
            raise RuntimeError(f"{name}: CUDA error {rc}")

    return call


api("cuInit", [c.c_uint])(0)
device = c.c_int()
api("cuDeviceGet", [c.POINTER(c.c_int), c.c_int])(c.byref(device), 0)
context = c.c_void_p()
api("cuCtxCreate_v2", [c.POINTER(c.c_void_p), c.c_uint, c.c_int])(
    c.byref(context), 0, device
)
ptx = b""".version 7.0
.target sm_75
.address_size 64
.visible .entry calculate(.param .u64 output, .param .u32 count) {
.reg .pred p;
.reg .b32 r<6>;
.reg .b64 a<3>;
ld.param.u64 a0, [output];
ld.param.u32 r0, [count];
mov.u32 r1, %ctaid.x;
mov.u32 r2, %ntid.x;
mov.u32 r3, %tid.x;
mad.lo.u32 r4, r1, r2, r3;
setp.ge.u32 p, r4, r0;
@p bra done;
mul.wide.u32 a1, r4, 4;
add.u64 a2, a0, a1;
mad.lo.u32 r5, r4, 3, 7;
st.global.u32 [a2], r5;
done:
ret;
}
"""
module = c.c_void_p()
api("cuModuleLoadData", [c.POINTER(c.c_void_p), c.c_void_p])(
    c.byref(module), c.cast(c.c_char_p(ptx), c.c_void_p)
)
function = c.c_void_p()
api("cuModuleGetFunction", [c.POINTER(c.c_void_p), c.c_void_p, c.c_char_p])(
    c.byref(function), module, b"calculate"
)
n = c.c_uint(65536)
pointer = c.c_uint64()
api("cuMemAlloc_v2", [c.POINTER(c.c_uint64), c.c_size_t])(c.byref(pointer), n.value * 4)
params = (c.c_void_p * 2)(c.addressof(pointer), c.addressof(n))
api(
    "cuLaunchKernel",
    [c.c_void_p] + [c.c_uint] * 7 + [c.c_void_p, c.POINTER(c.c_void_p), c.c_void_p],
)(function, 256, 1, 1, 256, 1, 1, 0, None, params, None)
api("cuCtxSynchronize", [])()
result = (c.c_uint * n.value)()
api("cuMemcpyDtoH_v2", [c.c_void_p, c.c_uint64, c.c_size_t])(
    result, pointer, n.value * 4
)
for i, value in enumerate(result):
    if value != i * 3 + 7:
        raise RuntimeError(f"GPU result mismatch at {i}: {value}")
api("cuMemFree_v2", [c.c_uint64])(pointer)
api("cuCtxDestroy_v2", [c.c_void_p])(context)
print(
    json.dumps(
        {
            "gpu_computation": "passed",
            "checked_elements": n.value,
            "operation": "out[i] = 3*i + 7",
        }
    )
)
