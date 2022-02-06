<?php

declare(strict_types=1);

namespace Yiisoft\Db\Oracle\Tests;

use Closure;
use InvalidArgumentException;
use Yiisoft\Arrays\ArrayHelper;
use Yiisoft\Db\Exception\NotSupportedException;
use Yiisoft\Db\Expression\Expression;
use Yiisoft\Db\Oracle\DDLCommand;
use Yiisoft\Db\Query\Query;

/**
 * @group oracle
 */
final class DDLCommandTest extends TestCase
{
    public function addDropChecksProvider(): array
    {
        $tableName = 'T_constraints_1';
        $name = 'CN_check';

        return [
            'drop' => [
                "ALTER TABLE {{{$tableName}}} DROP CONSTRAINT [[$name]]",
                static function (DDLCommand $ddl) use ($tableName, $name) {
                    return $ddl->dropCheck($name, $tableName);
                },
            ],
            'add' => [
                "ALTER TABLE {{{$tableName}}} ADD CONSTRAINT [[$name]] CHECK ([[C_not_null]] > 100)",
                static function (DDLCommand $ddl) use ($tableName, $name) {
                    return $ddl->addCheck($name, $tableName, '[[C_not_null]] > 100');
                },
            ],
        ];
    }

    /**
     * @dataProvider addDropChecksProvider
     *
     * @param string $sql
     * @param Closure $builder
     */
    public function testAddDropCheck(string $sql, Closure $builder): void
    {
        $db = $this->getConnection();
        $this->assertSame($db->getQuoter()->quoteSql($sql), $builder(new DDLCommand($db->getQuoter())));
    }

    public function testCommentColumn()
    {
        $db = $this->getConnection();
        $ddl = new DDLCommand($db->getQuoter());

        $expected = "COMMENT ON COLUMN [[comment]].[[text]] IS 'This is my column.'";
        $sql = $ddl->addCommentOnColumn('comment', 'text', 'This is my column.');
        $this->assertEquals($this->replaceQuotes($expected), $sql);

        $expected = "COMMENT ON COLUMN [[comment]].[[text]] IS ''";
        $sql = $ddl->dropCommentFromColumn('comment', 'text');
        $this->assertEquals($this->replaceQuotes($expected), $sql);
    }

    public function testCommentTable()
    {
        $db = $this->getConnection();
        $ddl = new DDLCommand($db->getQuoter());

        $expected = "COMMENT ON TABLE [[comment]] IS 'This is my table.'";
        $sql = $ddl->addCommentOnTable('comment', 'This is my table.');
        $this->assertEquals($this->replaceQuotes($expected), $sql);

        $expected = "COMMENT ON TABLE [[comment]] IS ''";
        $sql = $ddl->dropCommentFromTable('comment');
        $this->assertEquals($this->replaceQuotes($expected), $sql);
    }
}
