"use client";

import {
  StyledRoot,
  StyledTable,
  StyledTableBody,
  StyledTableHead,
  StyledTableHeadCell,
  StyledTableHeadRow,
} from "baseui/table-semantic";
import type { ReactNode } from "react";

export function Table({ label, headers, children, fill }: {
  label: string;
  /** Column headings; strings, or nodes when a host styles them. */
  headers: ReactNode[];
  children: ReactNode;
  fill?: boolean;
}) {
  return <StyledRoot tabIndex={0} role="region" aria-label={label} $style={fill ? { flex: "1 1 0px", height: 0, minHeight: "12rem", overscrollBehavior: "contain" } : {}}>
    <StyledTable aria-label={label}>
      <StyledTableHead><StyledTableHeadRow>
        {headers.map((header, index) => <StyledTableHeadCell key={index} scope="col">{header}</StyledTableHeadCell>)}
      </StyledTableHeadRow></StyledTableHead>
      <StyledTableBody>{children}</StyledTableBody>
    </StyledTable>
  </StyledRoot>;
}
