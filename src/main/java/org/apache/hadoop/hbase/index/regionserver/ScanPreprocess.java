package org.apache.hadoop.hbase.index.regionserver;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.index.client.Range;
import org.apache.hadoop.hbase.regionserver.HRegion;

public class ScanPreprocess {

  public static ConditionTree preprocess(HRegion region, Filter filter, float maxScale) {
    if (filter == null) return null;
    ConditionTree tree = null;
    if (isIndexFilter(region, filter)) {
      tree = new ConditionTreeNoneLeafNode(region, (SingleColumnValueFilter) filter, maxScale);
    } else if (filter instanceof FilterList) {
      tree = new ConditionTreeNoneLeafNode(region, (FilterList) filter, maxScale);
    }

    if (tree.isPrune()) {
      System.out.println("winter tree is prune");
      return null;
    } else {
      return tree;
    }
  }

  static boolean isIndexFilter(HRegion region, Filter filter) {
    return (filter instanceof SingleColumnValueFilter)
        && region.getAvaliableIndexes().contains(
          KeyValue.makeColumn(((SingleColumnValueFilter) filter).getFamily(),
            ((SingleColumnValueFilter) filter).getQualifier()));
  }

  public static abstract class ConditionTree implements Comparable<ConditionTree> {
    boolean prune = false;
    float scale = 0;
    Range range = null;
    Operator operator = null;
    List<ConditionTree> children = null;

    public boolean isPrune() {
      return prune;
    }

    public float getScale() {
      return this.scale;
    }

    public Range getRange() {
      return this.range;
    }

    public List<ConditionTree> getChildren() {
      return this.children;
    }

    public Operator getOperator() {
      return this.operator;
    }

    public int compareTo(ConditionTree tree) {
      float value = this.scale - tree.scale;
      if (value > -0.0000001 && value < 0.0000001) return 0;
      else if (value > 0) return 1;
      else return -1;
    }

  }

  public static class ConditionTreeLeafNode extends ConditionTree {
    public ConditionTreeLeafNode(HRegion region, Range range, float maxScale) {
      this.range = range;
      this.scale = region.getStore(range.getFamily()).getIndexScanScale(range);
      // prune large scale node
      if (this.scale > maxScale) {
        System.out.println("winter scale too big");
        this.prune = true;
      }
    }
  }

  public static class ConditionTreeNoneLeafNode extends ConditionTree {
    public ConditionTreeNoneLeafNode(HRegion region, SingleColumnValueFilter filter, float maxScale) {
      this.operator = Operator.MUST_PASS_ALL;
      this.children = new LinkedList<ConditionTree>();

      Range[] ranges = Range.fromFilter(filter);
      for (Range range : ranges) {
        ConditionTreeLeafNode node = new ConditionTreeLeafNode(region, range, maxScale);

        if (!node.isPrune()) {
          children.add(node);
        }
      }

      if (!children.isEmpty()) {
        this.scale = 1.0f;
        for (ConditionTree node : children) {
          this.scale *= node.getScale();
        }
      }

      if (children.isEmpty() || this.scale > maxScale) {
        this.prune = true;
      }

    }

    public ConditionTreeNoneLeafNode(HRegion region, FilterList flist, float maxScale) {
      this.operator = flist.getOperator();
      this.children = new LinkedList<ConditionTree>();

      LOOP: for (Filter filter : flist.getFilters()) {
        if (isIndexFilter(region, filter)) {
          Range[] ranges = Range.fromFilter((SingleColumnValueFilter) filter);
          for (Range range : ranges) {
            ConditionTreeLeafNode node = new ConditionTreeLeafNode(region, range, maxScale);
            if (!node.isPrune()) {
              children.add(node);
            } else if (this.operator == Operator.MUST_PASS_ONE) {
              this.prune = true;
              break LOOP;
            }
          }
        } else if (filter instanceof FilterList) {
          ConditionTreeNoneLeafNode node =
              new ConditionTreeNoneLeafNode(region, (FilterList) filter, maxScale);
          if (!node.isPrune()) {
            if (this.operator == node.getOperator() || node.getChildren().size() == 1) {
              children.addAll(node.getChildren());
            } else {
              children.add(node);
            }
          } else if (this.operator == Operator.MUST_PASS_ONE) {
            this.prune = true;
            break LOOP;
          }
        } else if (this.operator == Operator.MUST_PASS_ONE) {
          this.prune = true;
          break LOOP;
        }

        if (this.operator == Operator.MUST_PASS_ALL) {
          // TODO merge children condition
          // compute scale
          if (!children.isEmpty()) {
            Collections.sort(children);
            this.scale = 1.0f;
            for (ConditionTree node : children) {
              this.scale *= node.getScale();
            }
          }
          if (children.isEmpty() || this.scale > maxScale) {
            this.prune = true;
          }
        } else {
          if (!this.prune) {
            // TODO merge children condition
            // compute scale
            if (!children.isEmpty()) {
              Collections.sort(children);
              this.scale = 0.0f;
              for (ConditionTree node : children) {
                this.scale = this.scale + node.getScale() - this.scale * node.getScale();
              }
            }
            if (children.isEmpty() || this.scale > maxScale) {
              this.prune = true;
            }
          }
        }
      }
    }

    @Override
    public List<ConditionTree> getChildren() {
      return this.children;
    }

    @Override
    public Operator getOperator() {
      return this.operator;
    }

    @Override
    public float getScale() {
      return this.scale;
    }

  }

}
