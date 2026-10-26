"""Static check: every !Ref / !GetAtt / !Sub target in template.yaml exists."""
import re, sys, yaml

class L(yaml.SafeLoader): pass
refs, gets = [], []
def tag(name):
    def c(loader, node):
        v = loader.construct_scalar(node) if isinstance(node, yaml.ScalarNode) else \
            loader.construct_sequence(node, deep=True)
        if name == "Ref": refs.append(v)
        if name == "GetAtt": gets.append(v if isinstance(v, str) else ".".join(v))
        return {name: v}
    return c
for n in ("Ref", "GetAtt", "Sub"):
    L.add_constructor("!" + n, tag(n))

doc = yaml.load(open("template.yaml"), L)
res = set(doc["Resources"])
implicit = {"ServerlessHttpApi", "AWS::Region", "AWS::StackName", "AWS::AccountId"}
bad = [r for r in refs if r not in res and r not in implicit]
bad += [g for g in gets if g.split(".")[0] not in res and g.split(".")[0] not in implicit]
print(f"{len(res)} resources, {len(refs)} Refs, {len(gets)} GetAtts")
if bad:
    print("UNRESOLVED:", bad); sys.exit(1)

# handler paths exist
import os
for name, r in doc["Resources"].items():
    if r["Type"] == "AWS::Serverless::Function":
        p = r["Properties"]; mod = p["Handler"].split(".")[0]
        f = os.path.join(p["CodeUri"], mod + ".py")
        assert os.path.exists(f), f"{name}: missing {f}"
# SQS visibility >= 6x function timeout (30s)
for n in ("AnalyticsQueue", "AlertsQueue"):
    assert doc["Resources"][n]["Properties"]["VisibilityTimeout"] >= 180
print("template OK")
