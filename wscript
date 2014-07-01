import os

srcdir = "."
blddir = "build"
VERSION = "0.0.1"

def set_options(opt):
  opt.tool_options("compiler_cxx")

def configure(conf):
  conf.check_tool("compiler_cxx")
  conf.check_tool("node_addon")

def build_post(bld):
  module_path = bld.path.find_resource('tokyocabinet.node').abspath(bld.env)
  os.system('cp %r build/tokyocabinet.node' % module_path)
  
def build(bld):
  obj = bld.new_task_gen("cxx", "shlib", "node_addon")
  obj.target = "tokyocabinet"
  obj.source = "src/tokyocabinet.cc"
  obj.includes = ["."]
  obj.defines = "__STDC_LIMIT_MACROS"
  obj.lib = ["tokyocabinet"]
  bld.add_post_fun(build_post)
